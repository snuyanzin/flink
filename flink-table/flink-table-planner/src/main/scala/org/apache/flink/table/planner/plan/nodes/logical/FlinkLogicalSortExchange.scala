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
package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollation, RelDistribution, RelInput, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical.LogicalSortExchange
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.util
import java.util.Locale;

class FlinkLogicalSortExchange(input: RelInput)
  extends LogicalSortExchange(input)
  with FlinkLogicalRel {}

private class FlinkLogicalSortExchangeConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val se = rel.asInstanceOf[LogicalSortExchange]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(se.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalSortExchange(new RelInput() {
      override def getCluster: RelOptCluster = se.getCluster

      override def getTraitSet: RelTraitSet = se.getTraitSet

      override def getTable(table: String): RelOptTable = se.getTable

      override def getInput: RelNode = se.getInput

      override def getInputs: util.List[RelNode] = se.getInputs

      override def getExpression(tag: String): RexNode = null

      override def getBitSet(tag: String): ImmutableBitSet = null

      override def getBitSetList(tag: String): util.List[ImmutableBitSet] = null

      override def getAggregateCalls(tag: String): util.List[AggregateCall] = null

      override def get(tag: String): AnyRef = null

      override def getString(tag: String): String = null

      override def getFloat(tag: String): Float = 1f

      override def getEnum[E <: Enum[E]](tag: String, enumClass: Class[E]): E =
        Util.enumVal(enumClass, tag.toUpperCase(Locale.ROOT))

      override def getExpressionList(tag: String): util.List[RexNode] = null

      override def getStringList(tag: String): util.List[String] = null

      override def getIntegerList(tag: String): util.List[Integer] = null

      override def getIntegerListList(tag: String): util.List[util.List[Integer]] = null

      override def getRowType(tag: String): RelDataType = se.getRowType

      override def getRowType(expressionsTag: String, fieldsTag: String): RelDataType =
        se.getRowType

      override def getCollation: RelCollation = se.getCollation

      override def getDistribution: RelDistribution = se.getDistribution

      override def getTuples(tag: String): ImmutableList[ImmutableList[RexLiteral]] = null

      override def getBoolean(tag: String, default_ : Boolean): Boolean = false
    })
  }
}

object FlinkLogicalSortExchange {
  val CONVERTER: ConverterRule = new FlinkLogicalSortExchangeConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalSortExchange],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalSortExchangeConverter"))
}
