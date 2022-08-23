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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalLocalSortAggregate, BatchPhysicalSort, BatchPhysicalSortAggregate}

import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode

/**
 * There maybe exist a subTree like localSortAggregate -> globalSortAggregate, or localSortAggregate
 * -> sort -> globalSortAggregate which the middle shuffle is removed. The rule could remove
 * redundant localSortAggregate node.
 */
abstract class RemoveRedundantLocalSortAggRule(config: RelRule.Config)
  extends RelRule[RelRule.Config](config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalAgg = getOriginalGlobalAgg(call)
    val localAgg = getOriginalLocalAgg(call)
    val inputOfLocalAgg = getOriginalInputOfLocalAgg(call)
    val newGlobalAgg = new BatchPhysicalSortAggregate(
      globalAgg.getCluster,
      globalAgg.getTraitSet,
      inputOfLocalAgg,
      globalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      inputOfLocalAgg.getRowType,
      localAgg.grouping,
      localAgg.auxGrouping,
      // Use the localAgg agg calls because the global agg call filters was removed,
      // see BatchPhysicalSortAggRule for details.
      localAgg.getAggCallToAggFunction,
      isMerge = false)
    call.transformTo(newGlobalAgg)
  }

  private[table] def getOriginalGlobalAgg(call: RelOptRuleCall): BatchPhysicalSortAggregate

  private[table] def getOriginalLocalAgg(call: RelOptRuleCall): BatchPhysicalLocalSortAggregate

  private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode

}

class RemoveRedundantLocalSortAggWithoutSortRule(
    config: RemoveRedundantLocalSortAggWithoutSortRule.Config)
  extends RemoveRedundantLocalSortAggRule(config) {

  override private[table] def getOriginalGlobalAgg(
      call: RelOptRuleCall): BatchPhysicalSortAggregate = {
    call.rels(0).asInstanceOf[BatchPhysicalSortAggregate]
  }

  override private[table] def getOriginalLocalAgg(
      call: RelOptRuleCall): BatchPhysicalLocalSortAggregate = {
    call.rels(1).asInstanceOf[BatchPhysicalLocalSortAggregate]
  }

  override private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode = {
    call.rels(2)
  }

}

class RemoveRedundantLocalSortAggWithSortRule(
    config: RemoveRedundantLocalSortAggWithSortRule.Config)
  extends RemoveRedundantLocalSortAggRule(config) {

  override private[table] def getOriginalGlobalAgg(
      call: RelOptRuleCall): BatchPhysicalSortAggregate = {
    call.rels(0).asInstanceOf[BatchPhysicalSortAggregate]
  }

  override private[table] def getOriginalLocalAgg(
      call: RelOptRuleCall): BatchPhysicalLocalSortAggregate = {
    call.rels(2).asInstanceOf[BatchPhysicalLocalSortAggregate]
  }

  override private[table] def getOriginalInputOfLocalAgg(call: RelOptRuleCall): RelNode = {
    call.rels(3)
  }

}

object RemoveRedundantLocalSortAggRule {
  val WITHOUT_SORT = new RemoveRedundantLocalSortAggWithoutSortRule(
    RemoveRedundantLocalSortAggWithoutSortRule.Config.DEFAULT)
  val WITH_SORT = new RemoveRedundantLocalSortAggWithSortRule(
    RemoveRedundantLocalSortAggWithSortRule.Config.DEFAULT)
}

object RemoveRedundantLocalSortAggWithoutSortRule {
  object Config {
    val DEFAULT = RelRule.Config.EMPTY
      .withOperandSupplier(
        (b0: RelRule.OperandBuilder) =>
          b0.operand(classOf[BatchPhysicalSortAggregate])
            .oneInput(
              (b1: RelRule.OperandBuilder) =>
                b1.operand(classOf[BatchPhysicalLocalSortAggregate])
                  .oneInput(
                    (b2: RelRule.OperandBuilder) =>
                      b2.operand(classOf[RelNode])
                        .`trait`(FlinkConventions.BATCH_PHYSICAL)
                        .anyInputs())))
      .withDescription("RemoveRedundantLocalSortAggWithoutSortRule")
      .as(classOf[Config])
  }

  trait Config extends RelRule.Config {
    override def toRule = new RemoveRedundantLocalSortAggWithoutSortRule(this)
  }
}

object RemoveRedundantLocalSortAggWithSortRule {
  object Config {
    val DEFAULT = RelRule.Config.EMPTY
      .withOperandSupplier(
        (b0: RelRule.OperandBuilder) =>
          b0.operand(classOf[BatchPhysicalSortAggregate])
            .oneInput(
              (b1: RelRule.OperandBuilder) =>
                b1.operand(classOf[BatchPhysicalSort])
                  .oneInput(
                    (b2: RelRule.OperandBuilder) =>
                      b2.operand(classOf[BatchPhysicalLocalSortAggregate])
                        .oneInput(
                          (b3: RelRule.OperandBuilder) =>
                            b3.operand(classOf[BatchPhysicalLocalSortAggregate])
                              .`trait`(FlinkConventions.BATCH_PHYSICAL)
                              .anyInputs()))))
      .withDescription("RemoveRedundantLocalSortAggWithSortRule")
      .as(classOf[Config])
  }

  trait Config extends RelRule.Config {
    override def toRule = new RemoveRedundantLocalSortAggWithSortRule(this)
  }
}
