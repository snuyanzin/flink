/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;


/**
 * Rule for converting a cascade of predicates to [[IN]] or [[NOT_IN]].
 *
 * For example,
 *   1. convert predicate: (x = 1 OR x = 2 OR x = 3 OR x = 4) AND y = 5 to predicate: x IN (1, 2, 3,
 *      4) AND y = 5. 2. convert predicate: (x <> 1 AND x <> 2 AND x <> 3 AND x <> 4) AND y = 5 to
 *      predicate: x NOT IN (1, 2, 3, 4) AND y = 5.
 */
public class ConvertToNotInOrInRule
  extends RelRule { /*(operand(classOf[Filter], any), "ConvertToNotInOrInRule") { */

  // these threshold values are set by OptimizableHashSet benchmark test on different type.
  // threshold for non-float and non-double type
  private static final int THRESHOLD = 4;
  // threshold for float and double type
  private static final int FRACTIONAL_THRESHOLD = 20;

  public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
    RexNode condition = filter.getCondition();

    // convert equal expression connected by OR to IN
    val inExpr = convertToNotInOrIn(call.builder(), condition, IN);
    // convert not-equal expression connected by AND to NOT_IN
    val notInExpr = convertToNotInOrIn(call.builder(), inExpr.getOrElse(condition), NOT_IN)

    notInExpr match {
      case Some(expr) =>
        val newFilter = filter.copy(filter.getTraitSet, filter.getInput, expr)
        call.transformTo(newFilter)
      case _ =>
        // check IN conversion if NOT_IN conversion is fail
        inExpr match {
          case Some(expr) =>
            val newFilter = filter.copy(filter.getTraitSet, filter.getInput, expr)
            call.transformTo(newFilter)
          case _ => // do nothing
        }
    }
  }

  /** Returns a condition decomposed by [[AND]] or [[OR]]. */
  private List<RexNode> decomposedBy(RexNode rex, SqlBinaryOperator operator) {
      final SqlKind kind = operator.getKind();
      switch (kind) {
            case AND:
                return RelOptUtil.conjunctions(rex);
            case OR:
                return RelOptUtil.disjunctions(rex);
            default:
                throw new AssertionError("Unsupported operator " + kind);
      }
  }

  /**
   * Convert a cascade predicates to [[IN]] or [[NOT_IN]].
   *
   * @param builder
   *   The [[RelBuilder]] to build the [[RexNode]].
   * @param rex
   *   The predicates to be converted.
   * @return
   *   The converted predicates.
   */
  private Optional<RexNode> convertToNotInOrIn(
          RelBuilder builder,
          RexNode rex,
          SqlBinaryOperator toOperator) {

    // For example, when convert to [[IN]], fromOperator is [[EQUALS]].
    // We convert a cascade of [[EQUALS]] to [[IN]].
    // A connect operator is used to connect the fromOperator.
    // A composed operator may contains sub [[IN]] or [[NOT_IN]].
    val (fromOperator, connectOperator, composedOperator) = toOperator match {
      case IN => (EQUALS, OR, AND)
      case NOT_IN => (NOT_EQUALS, AND, OR)
    }

    val decomposed = decomposedBy(rex, connectOperator)
    val combineMap = new java.util.HashMap[String, mutable.ListBuffer[RexCall]]
    val rexBuffer = new mutable.ArrayBuffer[RexNode]
    var beenConverted = false

    // traverse decomposed predicates
    decomposed.foreach {
      case call: RexCall =>
        call.getOperator match {
          // put same predicates into combine map
          case `fromOperator` =>
            (call.operands(0), call.operands(1)) match {
              case (ref, _: RexLiteral) =>
                combineMap.getOrElseUpdate(ref.toString, mutable.ListBuffer[RexCall]()) += call
              case (l: RexLiteral, ref) =>
                combineMap.getOrElseUpdate(ref.toString, mutable.ListBuffer[RexCall]()) +=
                  call.clone(call.getType, List(ref, l))
              case _ => rexBuffer += call
            }

          // process sub predicates
          case `composedOperator` =>
            val newRex = decomposedBy(call, composedOperator).map {
              r =>
                convertToNotInOrIn(builder, r, toOperator) match {
                  case Some(ex) =>
                    beenConverted = true
                    ex
                  case None => r
                }
            }
            composedOperator match {
              case AND => rexBuffer += builder.and(newRex)
              case OR => rexBuffer += builder.or(newRex)
            }

          case _ => rexBuffer += call
        }

      case rex => rexBuffer += rex
    }

    combineMap.values.foreach {
      list =>
        if (needConvert(list.toList)) {
          val inputRef = list.head.getOperands.head
          val values = list.map(_.getOperands.last)
          val call = toOperator match {
            case IN => builder.getRexBuilder.makeIn(inputRef, values)
            case NOT_IN =>
              builder.getRexBuilder
                .makeCall(NOT, builder.getRexBuilder.makeIn(inputRef, values))
          }
          rexBuffer += call
          beenConverted = true
        } else {
          connectOperator match {
            case AND => rexBuffer += builder.and(list)
            case OR => rexBuffer += builder.or(list)
          }
        }
    }

    if (beenConverted) {
      // return result if has been converted
      connectOperator match {
        case AND => Some(builder.and(rexBuffer))
        case OR => Some(builder.or(rexBuffer))
      }
    } else {
      None
    }
  }

  private boolean needConvert(List<RexCall> rexNodes) {
    RexNode inputRef = rexNodes.get(0).getOperands().get(0);
    LogicalTypeRoot logicalTypeRoot = FlinkTypeFactory.toLogicalType(inputRef.getType()).getTypeRoot();
    switch (logicalTypeRoot) {
      case FLOAT:
      case DOUBLE:
        return rexNodes.size() >= FRACTIONAL_THRESHOLD;
      default:
        return rexNodes.size() >= THRESHOLD;
    }
  }
}

object ConvertToNotInOrInRule {
  val INSTANCE = new ConvertToNotInOrInRule
}
