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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank
import org.apache.flink.table.planner.plan.rules.physical.batch.RemoveRedundantLocalRankRule.Config

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelRule}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
 * Planner rule that matches a global [[BatchPhysicalRank]] on a local [[BatchPhysicalRank]], and
 * merge them into a global [[BatchPhysicalRank]].
 */
class RemoveRedundantLocalRankRule(config: Config) extends RelRule[Config](config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val globalRank: BatchPhysicalRank = call.rel(0)
    val localRank: BatchPhysicalRank = call.rel(1)
    globalRank.isGlobal && !localRank.isGlobal &&
    globalRank.rankType == localRank.rankType &&
    globalRank.partitionKey == localRank.partitionKey &&
    globalRank.orderKey == globalRank.orderKey &&
    globalRank.rankEnd == localRank.rankEnd
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalRank: BatchPhysicalRank = call.rel(0)
    val inputOfLocalRank: RelNode = call.rel(2)
    val newGlobalRank = globalRank.copy(globalRank.getTraitSet, List(inputOfLocalRank))
    call.transformTo(newGlobalRank)
  }
}

object RemoveRedundantLocalRankRule {
  val INSTANCE: RelOptRule = new RemoveRedundantLocalRankRule(Config.DEFAULT)

  object Config {
    val DEFAULT: Config = RelRule.Config.EMPTY
      .withOperandSupplier(
        (b0: RelRule.OperandBuilder) =>
          b0.operand(classOf[BatchPhysicalRank])
            .oneInput(
              (b1: RelRule.OperandBuilder) =>
                b1.operand(classOf[BatchPhysicalRank])
                  .oneInput(
                    (b2: RelRule.OperandBuilder) =>
                      b2.operand(classOf[RelNode])
                        .`trait`(FlinkConventions.BATCH_PHYSICAL)
                        .anyInputs())))
      .withDescription("RemoveRedundantLocalRankRule")
      .as(classOf[Config])
  }

  trait Config extends RelRule.Config {
    override def toRule = new RemoveRedundantLocalRankRule(this)
  }
}
