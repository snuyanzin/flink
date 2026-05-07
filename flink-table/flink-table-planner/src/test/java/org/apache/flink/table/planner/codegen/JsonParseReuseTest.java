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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that multiple JSON function calls on the same input reuse the parsed JSON. */
class JsonParseReuseTest {

    @RegisterExtension
    private final LoggerAuditingExtension codeLog =
            new LoggerAuditingExtension(CompileUtils.class, Level.DEBUG);

    private TableEnvironment tEnv;

    private static final String JSON_ROW1 =
            "{\"type\":\"account\",\"age\":42,\"address\":{\"city\":\"Munich\"},\"roles\":[\"user\",\"viewer\"]}";
    private static final String JSON_ROW2 =
            "{\"type\":\"admin\",\"age\":30,\"address\":{\"city\":\"Berlin\"},\"roles\":[\"admin\"]}";

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryView(
                "json_src", tEnv.fromValues(Row.of(JSON_ROW1), Row.of(JSON_ROW2)).as("json_data"));
    }

    private List<Row> collect(String sql) {
        TableResult result = tEnv.executeSql(sql);
        List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    private int countJsonParseInGeneratedCode() {
        Pattern pattern = Pattern.compile("jsonParse\\(");
        int total = 0;
        for (String msg : codeLog.getMessages()) {
            Matcher m = pattern.matcher(msg);
            while (m.find()) {
                total++;
            }
        }
        return total;
    }

    @Test
    void testTwoJsonValueCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), JSON_VALUE(json_data, '$.age') FROM json_src");
        assertThat(rows).containsExactlyInAnyOrder(Row.of("account", "42"), Row.of("admin", "30"));
        assertThat(countJsonParseInGeneratedCode())
                .as("Two JSON_VALUE calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testTwoJsonQueryCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_QUERY(json_data, '$.address'), "
                                + "JSON_QUERY(json_data, '$.roles' WITH WRAPPER) FROM json_src");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("{\"city\":\"Munich\"}", "[[\"user\",\"viewer\"]]"),
                        Row.of("{\"city\":\"Berlin\"}", "[[\"admin\"]]"));
        assertThat(countJsonParseInGeneratedCode())
                .as("Two JSON_QUERY calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testJsonValueAndJsonQueryMixed() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), "
                                + "JSON_QUERY(json_data, '$.address') FROM json_src");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "{\"city\":\"Berlin\"}"));
        assertThat(countJsonParseInGeneratedCode())
                .as("JSON_VALUE + JSON_QUERY on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testThreeJsonFunctionCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), "
                                + "JSON_VALUE(json_data, '$.age'), "
                                + "JSON_QUERY(json_data, '$.address') FROM json_src");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "42", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "30", "{\"city\":\"Berlin\"}"));
        assertThat(countJsonParseInGeneratedCode())
                .as("Three JSON function calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testThreeJsonFunctionCallsWithDifferentJson() {
        collect(
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_VALUE('{}', '$.age') FROM json_src");
        assertThat(countJsonParseInGeneratedCode()).as("Call for different JSON").isEqualTo(2);
    }
}
