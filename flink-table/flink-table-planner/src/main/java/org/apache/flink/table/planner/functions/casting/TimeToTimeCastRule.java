package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

class TimeToTimeCastRule extends AbstractExpressionCodeGeneratorCastRule<Number, Number> {

    static final TimeToTimeCastRule INSTANCE = new TimeToTimeCastRule();

    private TimeToTimeCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int inputPrecision = LogicalTypeChecks.getPrecision(inputLogicalType);
        int targetPrecision = LogicalTypeChecks.getPrecision(targetLogicalType);

        if (inputPrecision <= targetPrecision) {
            return inputTerm;
        } else {
            if (inputPrecision > 3) {
                return staticCall(
                        BuiltInMethods.TRUNCATE_SQL_TIME_WITH_NANOS(), inputTerm, targetPrecision);
            }
            return staticCall(BuiltInMethods.TRUNCATE_SQL_TIME(), inputTerm, targetPrecision);
        }
    }
}
