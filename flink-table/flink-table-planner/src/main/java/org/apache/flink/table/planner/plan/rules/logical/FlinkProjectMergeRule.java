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

import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extends calcite's ProjectMergeRule, modification: only merge the two neighbouring {@link
 * Project}s if each non-deterministic {@link RexNode} of bottom {@link Project} should appear at
 * most once in the project list of top {@link Project}.
 */
public class FlinkProjectMergeRule extends ProjectMergeRule {

    public static final RelOptRule INSTANCE = new FlinkProjectMergeRule(Config.DEFAULT);

    protected FlinkProjectMergeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);
        if (skipMerge(topProject, bottomProject)) {
            return false;
        }
        return FlinkRelUtil.isMergeable(topProject, bottomProject);
    }

    private boolean skipMerge(final Project topProject, final Project bottomProject) {
        Set<Integer> indexSet = new HashSet<>();
        List<RexNode> bottomProjects = bottomProject.getProjects();
        for (int i = 0; i < bottomProjects.size(); i++) {
            RexNode project = bottomProjects.get(i);
            if (project instanceof RexCall
                    && SqlKind.FUNCTION.contains(((RexCall) project).op.getKind())
                    && ((RexCall) project).op.isDeterministic()) {
                indexSet.add(i);
            }
        }
        if (indexSet.isEmpty()) {
            return false;
        }
        Set<RexNode> rexNodes = new HashSet<>();
        List<RexNode> topProjects = topProject.getProjects();
        for (RexNode rexNode : topProjects) {
            if (!(rexNode instanceof RexCall)) {
                continue;
            }
            RexCall rCall = (RexCall) rexNode;
            if (!(rCall.op instanceof SqlFunction)) {
                continue;
            }
            List<RexNode> operands = rCall.operands;
            for (RexNode op : operands) {
                if (op instanceof RexSlot) {
                    if (indexSet.contains(((RexSlot) op).getIndex()) && !rexNodes.add(op)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
