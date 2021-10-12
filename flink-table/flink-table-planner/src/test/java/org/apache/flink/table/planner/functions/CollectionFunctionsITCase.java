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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.map;

/** Tests for collections {@link BuiltInFunctionDefinitions}. */
public class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_KEYS)
                        .onFieldsWithData(
                                null,
                                "item",
                                Collections.singletonMap(1, "value"),
                                Collections.singletonMap(new Integer[] {1, 2}, "value"))
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()), DataTypes.STRING()))
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.INT()))
                                        .mapKeys(),
                                "MAP_KEYS(MAP[CAST(f0 AS BOOLEAN), CAST(f1 AS STRING)])",
                                new Boolean[] {null},
                                DataTypes.ARRAY(DataTypes.BOOLEAN()).notNull())
                        .testResult(
                                $("f2").mapKeys(),
                                "MAP_KEYS(f2)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f3").mapKeys(),
                                "MAP_KEYS(f3)",
                                new Integer[][] {new Integer[] {1, 2}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))));
    }

    // --------------------------------------------------------------------------------------------

    private static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2) {
        Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
