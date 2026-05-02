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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.coalesce;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;

/** Test {@link BuiltInFunctionDefinitions#COALESCE} and its return type. */
class CoalesceFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        final List<TestSetSpec> specs = new ArrayList<>();
        specs.addAll(allTypesBasic());
        specs.addAll(typePromotion());
        specs.addAll(lazyEvaluation());
        specs.addAll(constants());
        return specs.stream();
    }

    /**
     * For each supported type, verifies COALESCE returns the first non-null operand and infers a
     * NOT NULL result when at least one operand is NOT NULL.
     */
    private static List<TestSetSpec> allTypesBasic() {
        return Arrays.asList(
                basicSpec("BOOLEAN", BOOLEAN(), true, false),
                basicSpec("TINYINT", TINYINT(), (byte) 5, (byte) 10),
                basicSpec("SMALLINT", SMALLINT(), (short) 100, (short) 200),
                basicSpec("INT", INT(), 1, 2),
                basicSpec("BIGINT", BIGINT(), 100L, 200L),
                basicSpec("FLOAT", FLOAT(), 1.5f, 2.5f),
                basicSpec("DOUBLE", DOUBLE(), 1.5d, 2.5d),
                basicSpec(
                        "DECIMAL",
                        DECIMAL(5, 2),
                        new BigDecimal("123.45"),
                        new BigDecimal("234.56")),
                basicSpec("CHAR", CHAR(5), "hello", "world"),
                basicSpec("VARCHAR", VARCHAR(10), "hello", "world"),
                basicSpec("STRING", STRING(), "hello", "world"),
                basicSpec("BINARY", BINARY(2), new byte[] {0, 1}, new byte[] {2, 3}),
                basicSpec("VARBINARY", VARBINARY(5), new byte[] {0, 1, 2}, new byte[] {3, 4}),
                basicSpec("BYTES", BYTES(), new byte[] {0, 1, 2}, new byte[] {3, 4, 5}),
                basicSpec("DATE", DATE(), LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31)),
                basicSpec("TIME", TIME(), LocalTime.of(12, 34, 56), LocalTime.of(23, 59, 59)),
                basicSpec(
                        "TIMESTAMP",
                        TIMESTAMP(),
                        LocalDateTime.of(2026, 1, 1, 12, 0, 0),
                        LocalDateTime.of(2026, 12, 31, 23, 59, 59)),
                basicSpec(
                        "TIMESTAMP_LTZ",
                        TIMESTAMP_LTZ(),
                        Instant.parse("2026-01-01T12:00:00Z"),
                        Instant.parse("2026-12-31T23:59:59Z")),
                // INTERVAL types use simple resolutions (MONTH, SECOND) here because Calcite's
                // LEAST_RESTRICTIVE inference normalizes any compound year-month/day-time interval
                // down to the smallest unit when serialized through SQL, so the SQL/Table API
                // result types only agree on the simple form.
                basicSpec(
                        "INTERVAL_MONTH",
                        INTERVAL(MONTH()),
                        Period.ofMonths(18),
                        Period.ofMonths(27)),
                basicSpec(
                        "INTERVAL_SECOND",
                        INTERVAL(SECOND(3)),
                        Duration.ofMillis(12345),
                        Duration.ofMillis(67890)),
                basicSpec("ARRAY", ARRAY(INT()), new Integer[] {1, 2, 3}, new Integer[] {4, 5, 6}),
                basicSpec(
                        "MAP",
                        MAP(STRING(), INT()),
                        map(entry("a", 1), entry("b", 2)),
                        map(entry("c", 3), entry("d", 4))),
                rowSpec());
    }

    /**
     * Builds a TestSetSpec that exercises COALESCE for the given {@code type} with two non-null
     * sample values.
     *
     * <p>Verifies that:
     *
     * <ul>
     *   <li>COALESCE(null, V1) returns V1
     *   <li>COALESCE(V1, V2) returns V1 (first wins, second never inspected)
     *   <li>COALESCE(null, null, V2) returns V2 (skips through nulls)
     * </ul>
     */
    private static TestSetSpec basicSpec(String name, DataType type, Object value1, Object value2) {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE, name)
                .onFieldsWithData(null, value1, value2)
                .andDataTypes(type.nullable(), type.notNull(), type.notNull())
                .testResult(coalesce($("f0"), $("f1")), "COALESCE(f0, f1)", value1, type.notNull())
                .testResult(coalesce($("f1"), $("f2")), "COALESCE(f1, f2)", value1, type.notNull())
                .testResult(
                        coalesce($("f0"), $("f0"), $("f2")),
                        "COALESCE(f0, f0, f2)",
                        value2,
                        type.notNull());
    }

    private static TestSetSpec rowSpec() {
        DataType rowType = ROW(FIELD("a", INT()), FIELD("b", STRING()));
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE, "ROW")
                .onFieldsWithData(null, Row.of(1, "hello"), Row.of(2, "world"))
                .andDataTypes(rowType.nullable(), rowType.notNull(), rowType.notNull())
                .testResult(
                        coalesce($("f0"), $("f1")),
                        "COALESCE(f0, f1)",
                        Row.of(1, "hello"),
                        rowType.notNull())
                .testResult(
                        coalesce($("f1"), $("f2")),
                        "COALESCE(f1, f2)",
                        Row.of(1, "hello"),
                        rowType.notNull());
    }

    /**
     * Verifies the LEAST_RESTRICTIVE return-type inference combined with LEAST_NULLABLE: mixing
     * compatible operand types yields the widest type, and nullability is dropped if any operand is
     * NOT NULL.
     */
    private static List<TestSetSpec> typePromotion() {
        return Arrays.asList(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE, "INT and BIGINT")
                        .onFieldsWithData(null, 1, 2L)
                        .andDataTypes(INT().nullable(), INT().nullable(), BIGINT().notNull())
                        .testResult(
                                coalesce($("f0"), $("f1"), $("f2")),
                                "COALESCE(f0, f1, f2)",
                                1L,
                                BIGINT().notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE, "FLOAT and DOUBLE")
                        .onFieldsWithData(null, 1.0f, 2.0d)
                        .andDataTypes(FLOAT().nullable(), FLOAT().nullable(), DOUBLE().notNull())
                        .testResult(
                                coalesce($("f0"), $("f1"), $("f2")),
                                "COALESCE(f0, f1, f2)",
                                1.0d,
                                DOUBLE().notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE, "TINYINT and INT")
                        .onFieldsWithData(null, (byte) 7, 42)
                        .andDataTypes(TINYINT().nullable(), TINYINT().nullable(), INT().notNull())
                        .testResult(
                                coalesce($("f0"), $("f1"), $("f2")),
                                "COALESCE(f0, f1, f2)",
                                7,
                                INT().notNull()));
    }

    /**
     * Lazy evaluation: a non-null operand short-circuits the rest, so a {@link ThrowingFunction}
     * placed after it must NEVER be invoked. The negative-control case proves the throwing UDF
     * really does fire when the planner is forced to evaluate it.
     */
    private static List<TestSetSpec> lazyEvaluation() {
        return Arrays.asList(
                // First arg non-null at runtime: subsequent ThrowingFunction must NOT be called.
                // The first arg is declared nullable so the planner cannot simplify the call away
                // at compile time and the runtime short-circuit branch is exercised.
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.COALESCE,
                                "lazy: first operand non-null skips remainder")
                        .onFieldsWithData(1, 100)
                        .andDataTypes(INT().nullable(), INT().notNull())
                        .withFunction(ThrowingFunction.class)
                        .testResult(
                                coalesce($("f0"), call("ThrowingFunction", $("f1"))),
                                "COALESCE(f0, ThrowingFunction(f1))",
                                1,
                                INT().notNull()),
                // Middle arg non-null at runtime: ThrowingFunction in the third slot must NOT be
                // called, proving laziness extends across multiple operands.
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.COALESCE,
                                "lazy: middle operand non-null skips remainder")
                        .onFieldsWithData(null, 5, 100)
                        .andDataTypes(INT().nullable(), INT().nullable(), INT().notNull())
                        .withFunction(ThrowingFunction.class)
                        .testResult(
                                coalesce($("f0"), $("f1"), call("ThrowingFunction", $("f2"))),
                                "COALESCE(f0, f1, ThrowingFunction(f2))",
                                5,
                                INT().notNull()),
                // Negative control: the previous operand IS null at runtime, so ThrowingFunction
                // must be reached and must throw. Without this case the lazy tests above would
                // silently pass even if the throwing UDF were never wired up correctly.
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.COALESCE,
                                "negative control: throwing UDF fires when reached")
                        .onFieldsWithData(null, 100)
                        .andDataTypes(INT().nullable(), INT().notNull())
                        .withFunction(ThrowingFunction.class)
                        .testTableApiRuntimeError(
                                coalesce($("f0"), call("ThrowingFunction", $("f1"))),
                                "ThrowingFunction was called"));
    }

    /**
     * Original test cases preserved: nullability inference between fields and a literal constant.
     */
    private static List<TestSetSpec> constants() {
        return Collections.singletonList(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.COALESCE,
                                "constants and nullability inference")
                        .onFieldsWithData(null, null, 1)
                        .andDataTypes(BIGINT().nullable(), INT().nullable(), INT().notNull())
                        .testResult(
                                coalesce($("f0"), $("f1")),
                                "COALESCE(f0, f1)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                coalesce($("f0"), $("f2")),
                                "COALESCE(f0, f2)",
                                1L,
                                BIGINT().notNull())
                        .testResult(
                                coalesce($("f1"), $("f2")), "COALESCE(f1, f2)", 1, INT().notNull())
                        .testResult(
                                coalesce($("f0"), 1),
                                "COALESCE(f0, 1)",
                                1L,
                                // In this case, the return type is not null because we have a
                                // constant in the function invocation
                                BIGINT().notNull()));
    }

    /**
     * UDF that throws on every invocation. Used by {@link #lazyEvaluation()} to detect whether
     * COALESCE evaluated an operand that should have been short-circuited.
     */
    public static class ThrowingFunction extends ScalarFunction {
        public int eval(int i) {
            throw new RuntimeException("ThrowingFunction was called");
        }
    }
}
