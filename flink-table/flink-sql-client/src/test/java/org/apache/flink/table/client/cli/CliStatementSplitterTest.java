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

package org.apache.flink.table.client.cli;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Test {@link CliStatementSplitter}. */
@RunWith(Parameterized.class)
public class CliStatementSplitterTest {

    @Parameterized.Parameters(
            name = "Parsing: {0}. Expected identifier: {1}, Expected exception message: {2}")
    public static Object[][] parameters() {
        return new Object[][] {
            new Object[] {
                Arrays.asList(
                        "-- Define Table; \n"
                                + "CREATE TABLE MyTable (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'test-property' = 'test.value'\n);"
                                + "-- Define Table;",
                        "SET a = b;",
                        "\n" + "SELECT func(id) from MyTable\n;"),
                Arrays.asList(
                        "CREATE TABLE MyTable (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'test-property' = 'test.value'\n);",
                        "SET a = b;",
                        "SELECT func(id) from MyTable\n;")
            },
            new Object[] {
                Collections.singletonList("select 1; --comment  \n "),
                Collections.singletonList("select 1;")
            }
        };
    }

    @Parameterized.Parameter() public List<String> sqlLines;

    @Parameterized.Parameter(1)
    public List<String> expectedSplittedLines;

    @Test
    public void testSplitContent() {
        List<String> lines = sqlLines;
        List<String> actual = CliStatementSplitter.splitContent(String.join("\n", lines));
        for (int i = 0; i < lines.size(); i++) {
            assertEquals(expectedSplittedLines.get(i), actual.get(i));
        }
    }
}
