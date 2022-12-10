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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.immutables.value.Value;

import java.util.List;
import java.util.function.Predicate;

import static org.apache.flink.table.planner.plan.rules.logical.RemoveUnreachableCoalesceArgumentsRule.RemoveUnreachableCoalesceArgumentsRuleConfig.DEFAULT;

/**
 * Removes unreachable {@link BuiltInFunctionDefinitions#COALESCE} arguments.
 *
 * <p>An unreachable COALESCE argument is defined as any argument after the first argument in the
 * argument list with a non-null type.
 */
@Internal
public class RemoveUnreachableCoalesceArgumentsRule
        extends RelRule<
                RemoveUnreachableCoalesceArgumentsRule
                        .RemoveUnreachableCoalesceArgumentsRuleConfig> {

    public static final RelRule<
                    RemoveUnreachableCoalesceArgumentsRule
                            .RemoveUnreachableCoalesceArgumentsRuleConfig>
            PROJECT_INSTANCE = DEFAULT.withProject().toRule();
    public static final RelRule<
                    RemoveUnreachableCoalesceArgumentsRule
                            .RemoveUnreachableCoalesceArgumentsRuleConfig>
            FILTER_INSTANCE = DEFAULT.withFilter().toRule();
    public static final RelRule<
                    RemoveUnreachableCoalesceArgumentsRule
                            .RemoveUnreachableCoalesceArgumentsRuleConfig>
            JOIN_INSTANCE = DEFAULT.withJoin().toRule();
    public static final RelRule<
                    RemoveUnreachableCoalesceArgumentsRule
                            .RemoveUnreachableCoalesceArgumentsRuleConfig>
            CALC_INSTANCE = DEFAULT.withCalc().toRule();

    public RemoveUnreachableCoalesceArgumentsRule(
            RemoveUnreachableCoalesceArgumentsRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelNode relNode = call.rel(0);
        final RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        call.transformTo(
                relNode.accept(new UnreachableCoalesceArgumentsRemoveRexShuttle(rexBuilder)));
    }

    private static class UnreachableCoalesceArgumentsRemoveRexShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;

        private UnreachableCoalesceArgumentsRemoveRexShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);

            // Not a coalesce invocation, skip it
            if (!operatorIsCoalesce(call.getOperator())) {
                return call;
            }

            final int firstNonNullableArgIndex = getFirstNonNullableArgumentIndex(call);

            // If it's the first argument, just return the argument without the coalesce invocation
            if (firstNonNullableArgIndex == 0) {
                RexNode operand = call.operands.get(0);
                if (call.getType().equals(operand.getType())) {
                    return operand;
                } else {
                    return rexBuilder.makeCast(call.getType(), operand);
                }
            }

            // If it's the last argument, or no non-null argument was found, return the original
            // call
            if (firstNonNullableArgIndex == call.operands.size() - 1
                    || firstNonNullableArgIndex == -1) {
                return call;
            }

            // Return the coalesce invocation with a trimmed argument list
            final List<RexNode> trimmedOperandsList =
                    call.operands.subList(0, firstNonNullableArgIndex + 1);
            return call.clone(call.getType(), trimmedOperandsList);
        }

        private int getFirstNonNullableArgumentIndex(RexCall call) {
            for (int argIndex = 0; argIndex < call.operands.size(); argIndex++) {
                if (!call.operands.get(argIndex).getType().isNullable()) {
                    return argIndex;
                }
            }
            return -1;
        }
    }

    private static boolean hasCoalesceInvocation(RexNode node) {
        return FlinkRexUtil.hasOperatorCallMatching(
                node, RemoveUnreachableCoalesceArgumentsRule::operatorIsCoalesce);
    }

    private static boolean operatorIsCoalesce(SqlOperator op) {
        return op instanceof BridgingSqlFunction
                && ((BridgingSqlFunction) op)
                        .getDefinition()
                        .equals(BuiltInFunctionDefinitions.COALESCE);
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link RemoveUnreachableCoalesceArgumentsRule}. */
    @Value.Immutable(singleton = false)
    public interface RemoveUnreachableCoalesceArgumentsRuleConfig extends RelRule.Config {

        RemoveUnreachableCoalesceArgumentsRuleConfig DEFAULT =
                ImmutableRemoveUnreachableCoalesceArgumentsRuleConfig.builder()
                        .build()
                        .as(RemoveUnreachableCoalesceArgumentsRuleConfig.class);

        @Override
        default RelRule toRule() {
            return new RemoveUnreachableCoalesceArgumentsRule(this);
        }

        default RemoveUnreachableCoalesceArgumentsRuleConfig withProject() {
            Predicate<Project> projectPredicate =
                    lp ->
                            lp.getProjects().stream()
                                    .anyMatch(
                                            RemoveUnreachableCoalesceArgumentsRule
                                                    ::hasCoalesceInvocation);
            final RelRule.OperandTransform projectTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(Project.class)
                                    .predicate(projectPredicate)
                                    .anyInputs();

            return withOperandSupplier(projectTransform)
                    .as(RemoveUnreachableCoalesceArgumentsRuleConfig.class);
        }

        default RemoveUnreachableCoalesceArgumentsRuleConfig withFilter() {
            Predicate<Filter> filterPredicate =
                    lf ->
                            RemoveUnreachableCoalesceArgumentsRule.hasCoalesceInvocation(
                                    lf.getCondition());
            final RelRule.OperandTransform filterTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(Filter.class)
                                    .predicate(filterPredicate)
                                    .anyInputs();

            return withOperandSupplier(filterTransform)
                    .as(RemoveUnreachableCoalesceArgumentsRuleConfig.class);
        }

        default RemoveUnreachableCoalesceArgumentsRuleConfig withJoin() {
            Predicate<Join> joinPredicate =
                    lj ->
                            RemoveUnreachableCoalesceArgumentsRule.hasCoalesceInvocation(
                                    lj.getCondition());
            final RelRule.OperandTransform joinTransform =
                    operandBuilder ->
                            operandBuilder.operand(Join.class).predicate(joinPredicate).anyInputs();

            return withOperandSupplier(joinTransform)
                    .as(RemoveUnreachableCoalesceArgumentsRuleConfig.class);
        }

        default RemoveUnreachableCoalesceArgumentsRuleConfig withCalc() {
            Predicate<Calc> calcPredicate =
                    lc ->
                            lc.getProgram().getExprList().stream()
                                    .anyMatch(
                                            RemoveUnreachableCoalesceArgumentsRule
                                                    ::hasCoalesceInvocation);
            final RelRule.OperandTransform joinTransform =
                    operandBuilder ->
                            operandBuilder.operand(Calc.class).predicate(calcPredicate).anyInputs();

            return withOperandSupplier(joinTransform)
                    .as(RemoveUnreachableCoalesceArgumentsRuleConfig.class);
        }
    }
}
