package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class UpsertLeftJoinTombstoneProbeITCase extends StreamingTestBase {

    @Test
    void testTombstoneOnNestedNotNullCompositeNeverReachesSinkAsNull() throws Exception {
        final String id =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                TestValuesTableFactory.changelogRow("+U", 1, new Integer[] {1, 2}),
                                TestValuesTableFactory.changelogRow("+U", 2, new Integer[] {3}),
                                // tombstone: key only, NOT NULL value column is null
                                TestValuesTableFactory.changelogRow("-D", 1, null)));

        tEnv().executeSql(
                "CREATE TABLE UpsertSrc (k INT, v ARRAY<INT> NOT NULL, "
                        + "PRIMARY KEY (k) NOT ENFORCED) WITH ("
                        + "'connector' = 'values', 'data-id' = '"
                        + id
                        + "', 'changelog-mode' = 'UA,D')");
        tEnv().executeSql(
                "CREATE TABLE UpsertSnk (k INT, r ROW<a INT, b ARRAY<INT>>, "
                        + "PRIMARY KEY (k) NOT ENFORCED) WITH ("
                        + "'connector' = 'values', 'sink-insert-only' = 'false')");

        tEnv().executeSql("INSERT INTO UpsertSnk SELECT k, ROW(k, v) FROM UpsertSrc").await();

        // k=1 was inserted then deleted by the tombstone; k=2 survives.
        assertThat(TestValuesTableFactory.getResultsAsStrings("UpsertSnk"))
                .containsExactlyInAnyOrder("+I[2, +I[2, [3]]]");
    }
}
