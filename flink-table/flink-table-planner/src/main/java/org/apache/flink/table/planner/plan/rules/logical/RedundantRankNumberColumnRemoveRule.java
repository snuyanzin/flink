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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that removes the output column of rank number iff the rank number column is not used
 * by successor calc.
 */
public class RedundantRankNumberColumnRemoveRule
        extends RelRule<RedundantRankNumberColumnRemoveRule.Config> {
    public static final RedundantRankNumberColumnRemoveRule INSTANCE =
            new RedundantRankNumberColumnRemoveRule(Config.DEFAULT);

    public RedundantRankNumberColumnRemoveRule(Config config) {
        super(config);
    }

    /** Config for RedundantRankNumberColumnRemoveRule. */
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCalc.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRank.class)
                                                                        .noInputs()))
                        .withDescription("RedundantRankNumberColumnRemoveRule")
                        .as(RedundantRankNumberColumnRemoveRule.Config.class);

        @Override
        default RedundantRankNumberColumnRemoveRule toRule() {
            return new RedundantRankNumberColumnRemoveRule(this);
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        ImmutableBitSet usedFields = getUsedFields(calc.getProgram());
        FlinkLogicalRank rank = call.rel(1);
        return rank.outputRankNumber() && !usedFields.get(rank.getRowType().getFieldCount() - 1);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalRank rank = call.rel(1);
        FlinkLogicalRank newRank =
                new FlinkLogicalRank(
                        rank.getCluster(),
                        rank.getTraitSet(),
                        rank.getInput(),
                        rank.partitionKey(),
                        rank.orderKey(),
                        rank.rankType(),
                        rank.rankRange(),
                        rank.rankNumberType(),
                        false);
        RexProgram oldProgram = calc.getProgram();
        Pair<List<RexNode>, RexNode> projectsAndCondition = getProjectsAndCondition(oldProgram);
        RexProgram newProgram =
                RexProgram.create(
                        newRank.getRowType(),
                        projectsAndCondition.left,
                        projectsAndCondition.right,
                        oldProgram.getOutputRowType(),
                        rank.getCluster().getRexBuilder());
        FlinkLogicalCalc newCalc = FlinkLogicalCalc.create(newRank, newProgram);
        call.transformTo(newCalc);
    }

    private ImmutableBitSet getUsedFields(RexProgram program) {
        Pair<List<RexNode>, RexNode> projectsAndCondition = getProjectsAndCondition(program);
        return RelOptUtil.InputFinder.bits(projectsAndCondition.left, projectsAndCondition.right);
    }

    private Pair<List<RexNode>, RexNode> getProjectsAndCondition(RexProgram program) {
        List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        RexNode condition = null;
        if (program.getCondition() != null) {
            condition = program.expandLocalRef(program.getCondition());
        }
        return Pair.of(projects, condition);
    }
}
