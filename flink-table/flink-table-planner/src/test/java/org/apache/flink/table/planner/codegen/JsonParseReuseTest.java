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
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that multiple JSON function calls on the same input reuse the parsed JSON. */
class JsonParseReuseTest {

    private TableEnvironment tEnv;

    private static final String JSON_DATA =
            "{\"type\":\"account\",\"age\":42,\"address\":{\"city\":\"Munich\"},\"roles\":[\"user\",\"viewer\"]}";

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryView("json_src", tEnv.fromValues(Row.of(JSON_DATA)).as("json_data"));
    }

    private List<Row> collect(String sql) {
        TableResult result = tEnv.executeSql(sql);
        List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    @Test
    void testTwoJsonValueCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), JSON_VALUE(json_data, '$.age') FROM json_src");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("account");
        assertThat(rows.get(0).getField(1)).isEqualTo("42");
    }

    @Test
    void testTwoJsonQueryCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_QUERY(json_data, '$.address'), "
                                + "JSON_QUERY(json_data, '$.roles' WITH WRAPPER) FROM json_src");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("{\"city\":\"Munich\"}");
        assertThat(rows.get(0).getField(1)).isEqualTo("[[\"user\",\"viewer\"]]");
    }

    @Test
    void testJsonValueAndJsonQueryMixed() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), "
                                + "JSON_QUERY(json_data, '$.address') FROM json_src");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("account");
        assertThat(rows.get(0).getField(1)).isEqualTo("{\"city\":\"Munich\"}");
    }

    @Test
    void testThreeJsonFunctionCalls() {
        List<Row> rows =
                collect(
                        "SELECT JSON_VALUE(json_data, '$.type'), "
                                + "JSON_VALUE(json_data, '$.age'), "
                                + "JSON_QUERY(json_data, '$.address') FROM json_src");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("account");
        assertThat(rows.get(0).getField(1)).isEqualTo("42");
        assertThat(rows.get(0).getField(2)).isEqualTo("{\"city\":\"Munich\"}");
    }
}
