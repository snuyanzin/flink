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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for [[FlinkLimit0RemoveRule]]. */
class FlinkLimit0RemoveRuleTest extends TableTestBase {

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        FlinkChainedProgram<BatchOptimizeContext> programs = new FlinkChainedProgram<>();
        programs.addLast(
                "rules",
                FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(
                                RuleSets.ofList(
                                        FlinkSubQueryRemoveRule.FILTER(),
                                        FlinkLimit0RemoveRule.INSTANCE))
                        .build());
        util.replaceBatchProgram(programs);

        util.addTableSource(
                "MyTable",
                new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                new String[] {"a", "b", "c"});
    }

    @Test
    void testSimpleLimitZero() {
        util.verifyRelPlan("SELECT * FROM MyTable LIMIT 0");
    }

    @Test
    void testLimitZeroWithOrderBy() {
        util.verifyRelPlan("SELECT * FROM MyTable ORDER BY a LIMIT 0");
    }

    @Test
    void testLimitZeroWithOffset() {
        util.verifyRelPlan("SELECT * FROM MyTable ORDER BY a LIMIT 0 OFFSET 10");
    }

    @Test
    void testLimitZeroWithSelect() {
        util.verifyRelPlan("SELECT * FROM (SELECT a FROM MyTable LIMIT 0)");
    }

    @Test
    void testLimitZeroWithIn() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE a IN (SELECT a FROM MyTable LIMIT 0)");
    }

    @Test
    void testLimitZeroWithNotIn() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE a NOT IN (SELECT a FROM MyTable LIMIT 0)");
    }

    @Test
    void testLimitZeroWithExists() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE EXISTS (SELECT a FROM MyTable LIMIT 0)");
    }

    @Test
    void testLimitZeroWithNotExists() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable WHERE NOT EXISTS (SELECT a FROM MyTable LIMIT 0)");
    }

    @Test
    void testLimitZeroWithJoin() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable INNER JOIN (SELECT * FROM MyTable LIMIT 0) ON TRUE");
    }
}
