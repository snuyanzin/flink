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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/**
 * Rule to push the {@link FlinkLogicalWatermarkAssigner} into the {@link
 * FlinkLogicalTableSourceScan}.
 */
public class PushWatermarkIntoTableSourceScanRule
        extends PushWatermarkIntoTableSourceScanRuleBase<
                PushWatermarkIntoTableSourceScanRule.Config> {
    public static final PushWatermarkIntoTableSourceScanRule INSTANCE =
            new PushWatermarkIntoTableSourceScanRule(Config.DEFAULT);

    public PushWatermarkIntoTableSourceScanRule(Config config) {
        super(config);
    }

    /** Config for PushWatermarkIntoTableSourceScanRule. */
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalWatermarkAssigner.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(
                                                                                FlinkLogicalTableSourceScan
                                                                                        .class)
                                                                        .noInputs()))
                        .withDescription("PushWatermarkIntoTableSourceScanRule")
                        .as(Config.class);

        @Override
        default PushWatermarkIntoTableSourceScanRule toRule() {
            return new PushWatermarkIntoTableSourceScanRule(this);
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalTableSourceScan scan = call.rel(1);
        return supportsWatermarkPushDown(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalWatermarkAssigner watermarkAssigner = call.rel(0);
        FlinkLogicalTableSourceScan scan = call.rel(1);

        FlinkLogicalTableSourceScan newScan =
                getNewScan(
                        watermarkAssigner,
                        watermarkAssigner.watermarkExpr(),
                        scan,
                        ShortcutUtils.unwrapContext(scan).getTableConfig(),
                        true); // useWatermarkAssignerRowType

        call.transformTo(newScan);
    }
}
