package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SargJsonPlanITCase extends JsonPlanTestBase {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        List<Row> data =
                Arrays.asList(Row.of(1), Row.of(2), Row.of((Integer) null), Row.of(4), Row.of(5));
        createTestValuesSourceTable("MyTable", data, "a int");
        createTestNonInsertOnlyValuesSinkTable("`result`", "a int");
        String sql =
                "insert into `result` SELECT a\n"
                        + "FROM MyTable WHERE a = 1 OR a = 2 OR a IS NOT NULL";
        compileSqlAndExecutePlan(sql).await();
        List<String> expected = Arrays.asList("+I[1]", "+I[2]", "+I[null]");
        assertResult(expected, TestValuesTableFactory.getResults("result"));
    }
}
