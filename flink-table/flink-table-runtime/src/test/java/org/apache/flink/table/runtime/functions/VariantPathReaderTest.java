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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests that {@link VariantPathReader} reads what {@code PARSE_JSON} and access would. */
class VariantPathReaderTest {

    // objects, arrays, a path inside another, the same path twice, and steps of both kinds
    private static final Object[][] PATHS = {
        {"book", "title"},
        {"book"},
        {"book", "tags", 1},
        {"book", "title"},
        {"a", 0},
        {"a", 0, "b"},
        {0},
        {0, "x"},
        {"missing", "x"},
        {"?"},
        {"k" + (char) 0xD800}
    };

    private static final String BOOK =
            "{\"store\":\"Main\",\"book\":{\"author\":\"Melville\",\"title\":\"Moby Dick\","
                    + "\"tags\":[\"novel\",\"sea\"],\"year\":1851},\"open\":true}";

    private static Stream<Arguments> documents() {
        final String[] jsons = {
            BOOK,
            // values of other types at the steps
            "{\"book\":42}",
            "{\"book\":null}",
            "{\"book\":\"text\"}",
            "{\"book\":[1,2]}",
            "{\"book\":{\"title\":{\"nested\":[1,{\"x\":2}]}}}",
            "{\"a\":[{\"b\":1},{\"b\":2}]}",
            "{\"a\":[]}",
            "{\"a\":{\"0\":1}}",
            "[{\"x\":1},2]",
            "[]",
            "\"scalar\"",
            "null",
            "1",
            // duplicate keys, where the last one wins if they are allowed
            "{\"book\":{\"title\":\"first\"},\"book\":{\"title\":\"second\"}}",
            "{\"book\":{\"title\":\"first\"},\"book\":7}",
            "{\"store\":{\"x\":1,\"x\":2},\"book\":{\"title\":\"t\"}}",
            // keys that are stored as the same bytes
            "{\"?\":1,\"\\ud800\":2}",
            "{\"k\\ud800\":1}",
            // an escaped key of a path
            "{\"\\u0062ook\":{\"title\":\"t\"}}",
            // more keys than are compared one by one, with and without a duplicate after them
            manyKeys(20, null),
            manyKeys(20, "k17"),
            manyKeys(20, "k3"),
            // numbers that fail or change type in the skipped part
            "{\"store\":1e400,\"book\":{\"title\":\"t\"}}",
            "{\"store\":123456789012345678901234567890,\"book\":{\"title\":\"t\"}}",
            "{\"store\":-0.0,\"book\":{\"title\":1.5e3}}",
            // malformed documents
            "",
            "{",
            "{\"book\":}",
            "{\"book\":{\"title\":\"t\"},}",
            "[1,]",
            // content after the document, which the full parse ignores
            "{\"book\":{\"title\":\"t\"}} trailing",
            // a large skipped value
            largeDocument()
        };
        return Stream.of(jsons)
                .flatMap(
                        json ->
                                Stream.of(
                                        arguments(json.getBytes(StandardCharsets.UTF_8), false),
                                        arguments(json.getBytes(StandardCharsets.UTF_8), true)));
    }

    @ParameterizedTest(name = "[{index}] allowDuplicateKeys={1}")
    @MethodSource("documents")
    void testReadsWhatTheFullParseReads(byte[] json, boolean allowDuplicateKeys) {
        assertSame(json, allowDuplicateKeys);
    }

    @ParameterizedTest(name = "[{index}] allowDuplicateKeys={1}")
    @MethodSource("invalidUtf8")
    void testInvalidUtf8InSkippedString(byte[] json, boolean allowDuplicateKeys) {
        assertSame(json, allowDuplicateKeys);
    }

    private static Stream<Arguments> invalidUtf8() {
        final byte[] prefix = "{\"store\":\"".getBytes(StandardCharsets.UTF_8);
        final byte[] suffix = "\",\"book\":{\"title\":\"t\"}}".getBytes(StandardCharsets.UTF_8);
        final byte[] json = new byte[prefix.length + 1 + suffix.length];
        System.arraycopy(prefix, 0, json, 0, prefix.length);
        json[prefix.length] = (byte) 0xC3;
        System.arraycopy(suffix, 0, json, prefix.length + 1, suffix.length);
        return Stream.of(arguments(json, false), arguments(json, true));
    }

    private static void assertSame(byte[] json, boolean allowDuplicateKeys) {
        final VariantPathReader reader = new VariantPathReader(PATHS);
        Variant full = null;
        Exception fullError = null;
        try {
            full = BinaryVariantInternalBuilder.parseJson(json, allowDuplicateKeys);
        } catch (Exception e) {
            fullError = e;
        }
        Exception readError = null;
        try {
            reader.read(BinaryStringData.fromBytes(json), allowDuplicateKeys);
        } catch (Exception e) {
            readError = e;
        }
        if (fullError != null) {
            assertThat(readError).isNotNull();
            assertThat(readError.getClass()).isEqualTo(fullError.getClass());
            assertThat(readError.getMessage()).isEqualTo(fullError.getMessage());
            return;
        }
        assertThat(readError).isNull();
        for (int i = 0; i < PATHS.length; i++) {
            assertPath(full, PATHS[i], reader, i);
        }
    }

    // the path applied to the whole document, as the generated [key] and [index] access applies it
    private static void assertPath(Variant full, Object[] path, VariantPathReader reader, int i) {
        Variant expected = full;
        String expectedError = null;
        for (Object step : path) {
            if (step instanceof Integer) {
                if (!expected.isArray()) {
                    expectedError =
                            "Integer index access on variant requires an array variant, but a non-array variant was provided.";
                    break;
                }
                expected = expected.getElement((Integer) step);
            } else {
                if (!expected.isObject()) {
                    expectedError =
                            "String key access on variant requires an object variant, but a non-object variant was provided.";
                    break;
                }
                expected = expected.getField((String) step);
            }
            if (expected == null) {
                break;
            }
        }
        final String description = Arrays.toString(path);
        if (expectedError != null) {
            try {
                reader.get(i);
            } catch (TableRuntimeException e) {
                assertThat(e.getMessage()).as(description).isEqualTo(expectedError);
                return;
            }
            throw new AssertionError("expected an error for " + description);
        }
        final Variant actual = reader.get(i);
        if (expected == null) {
            assertThat(actual).as(description).isNull();
        } else {
            assertThat(actual).as(description).isNotNull();
            // the values have field name dictionaries of their own, so they are compared as JSON
            assertThat(actual.toJson()).as(description).isEqualTo(expected.toJson());
        }
    }

    // an object of count keys, then the book, and the key again if not null
    private static String manyKeys(int count, String repeated) {
        final StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < count; i++) {
            sb.append("\"k").append(i).append("\":").append(i).append(',');
        }
        sb.append("\"book\":{\"title\":\"t\"}");
        if (repeated != null) {
            sb.append(",\"").append(repeated).append("\":0");
        }
        return sb.append('}').toString();
    }

    private static String largeDocument() {
        final StringBuilder sb = new StringBuilder("{\"store\":\"");
        while (sb.length() < 300_000) {
            sb.append("xxxxxxxxxx");
        }
        return sb.append("\",\"book\":{\"title\":\"t\"}}").toString();
    }
}
