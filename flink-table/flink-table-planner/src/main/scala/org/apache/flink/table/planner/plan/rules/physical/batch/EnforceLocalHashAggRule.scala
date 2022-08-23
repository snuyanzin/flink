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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalExchange, BatchPhysicalExpand, BatchPhysicalHashAggregate}

import org.apache.calcite.plan.{RelOptRuleCall, RelRule}

/**
 * An [[EnforceLocalAggRuleBase]] that matches [[BatchPhysicalHashAggregate]]
 *
 * for example: select count(*) from t group by rollup (a, b) The physical plan
 *
 * {{{
 * HashAggregate(isMerge=[false], groupBy=[a, b, $e], select=[a, b, $e, COUNT(*)])
 * +- Exchange(distribution=[hash[a, b, $e]])
 *    +- Expand(projects=[{a=[$0], b=[$1], $e=[0]},
 *                        {a=[$0], b=[null], $e=[1]},
 *                        {a=[null], b=[null], $e=[3]}])
 * }}}
 *
 * will be rewritten to
 *
 * {{{
 * HashAggregate(isMerge=[true], groupBy=[a, b, $e], select=[a, b, $e, Final_COUNT(count1$0)])
 * +- Exchange(distribution=[hash[a, b, $e]])
 *    +- LocalHashAggregate(groupBy=[a, b, $e], select=[a, b, $e, Partial_COUNT(*) AS count1$0]
 *       +- Expand(projects=[{a=[$0], b=[$1], $e=[0]},
 *                           {a=[$0], b=[null], $e=[1]},
 *                           {a=[null], b=[null], $e=[3]}])
 * }}}
 */
class EnforceLocalHashAggRule(config: EnforceLocalHashAggRule.Config)
  extends EnforceLocalAggRuleBase(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: BatchPhysicalHashAggregate = call.rel(0)
    val expand: BatchPhysicalExpand = call.rel(2)

    val enableTwoPhaseAgg = isTwoPhaseAggEnabled(agg)

    val grouping = agg.grouping
    val constantShuffleKey = hasConstantShuffleKey(grouping, expand)

    grouping.nonEmpty && enableTwoPhaseAgg && constantShuffleKey
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: BatchPhysicalHashAggregate = call.rel(0)
    val expand: BatchPhysicalExpand = call.rel(2)

    val localAgg = createLocalAgg(agg, expand)
    val exchange = createExchange(agg, localAgg)
    val globalAgg = createGlobalAgg(agg, exchange)
    call.transformTo(globalAgg)
  }

}

object EnforceLocalHashAggRule {
  val INSTANCE = new EnforceLocalHashAggRule(Config.DEFAULT)

  object Config {
    val DEFAULT = RelRule.Config.EMPTY
      .withOperandSupplier(
        (b0: RelRule.OperandBuilder) =>
          b0.operand(classOf[BatchPhysicalHashAggregate])
            .oneInput(
              (b1: RelRule.OperandBuilder) =>
                b1.operand(classOf[BatchPhysicalExchange])
                  .oneInput(
                    (b2: RelRule.OperandBuilder) =>
                      b2.operand(classOf[BatchPhysicalExpand])
                        .anyInputs())))
      .withDescription("EnforceLocalHashAggRule")
      .as(classOf[Config])
  }

  trait Config extends RelRule.Config {
    override def toRule = new EnforceLocalHashAggRule(this)
  }
}
