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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;

import org.apache.commons.lang3.tuple.Pair;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.commons.lang3.tuple.Pair.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for {@link PromptHandler}. */
class PromptHandlerTest {
    private static final String CURRENT_CATALOG = "current_catalog";
    private static final String CURRENT_DATABASE = "current_database";
    private static final Terminal TERMINAL;

    static {
        try {
            TERMINAL = TerminalBuilder.terminal();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<Arguments> promptSupplier() {
        return Stream.of(
                of(TestSpec.forPromptPattern(toAnsi("")).expectedPromptValue("")),
                of(
                        TestSpec.forPromptPattern(toAnsi("simple_prompt"))
                                .expectedPromptValue("simple_prompt")),
                of(TestSpec.forPromptPattern(toAnsi("\\")).expectedPromptValue("")),
                of(TestSpec.forPromptPattern(toAnsi("\\\\")).expectedPromptValue("\\")),
                of(TestSpec.forPromptPattern(toAnsi("\\\\\\")).expectedPromptValue("\\")),
                of(TestSpec.forPromptPattern(toAnsi("\\\\\\\\")).expectedPromptValue("\\\\")),
                of(
                        TestSpec.forPromptPattern(toAnsi(of("", DEFAULT.foreground(RED).bold())))
                                .expectedPromptValue("")),
                of(TestSpec.forPromptPattern(toAnsi("\\d")).expectedPromptValue(CURRENT_DATABASE)),
                of(TestSpec.forPromptPattern(toAnsi("\\c")).expectedPromptValue(CURRENT_CATALOG)),
                of(
                        TestSpec.forPromptPattern(
                                        toAnsi(
                                                of("\\", DEFAULT),
                                                of(
                                                        "my prompt",
                                                        DEFAULT.foreground(AttributedStyle.GREEN)
                                                                .underline())))
                                .expectedPromptValue(
                                        toAnsi(
                                                of(
                                                        "my prompt",
                                                        DEFAULT.foreground(AttributedStyle.GREEN)
                                                                .underline())))),
                // property value in prompt
                of(
                        TestSpec.forPromptPattern(toAnsi("\\:my_prop\\:>"))
                                .propertyKey("my_prop")
                                .propertyValue("my_prop_value")
                                .expectedPromptValue("my_prop_value>")),
                // escaping of backslash \
                of(
                        TestSpec.forPromptPattern(
                                        toAnsi(
                                                of(
                                                        "\\\\[b:y,italic\\]\\:test\\:\\[default\\]>",
                                                        DEFAULT)))
                                .propertyKey("test")
                                .propertyValue("my_prop_value")
                                .expectedPromptValue("\\[b:y,italic]my_prop_value>")),
                // not specified \X will be handled as X
                of(TestSpec.forPromptPattern(toAnsi("\\X>")).expectedPromptValue("X>")),
                // if any of patterns \[...\], \{...\}, \:...\: not closed it will be handled as \X
                of(
                        TestSpec.forPromptPattern(toAnsi("\\{ \\:my_another_prop\\:\\[default\\]>"))
                                .propertyKey("my_another_prop")
                                .propertyValue("my_another_prop_value")
                                .expectedPromptValue("{ my_another_prop_value>")),
                of(
                        TestSpec.forPromptPattern(toAnsi("\\[default]\\:my_another_prop\\:>"))
                                .propertyKey("my_another_prop")
                                .propertyValue("my_another_prop_value")
                                .expectedPromptValue("[default]my_another_prop_value>")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("promptSupplier")
    public void promptTest(TestSpec spec) {
        PromptHandler promptHandler =
                new PromptHandler(
                        new Executor() {

                            @Override
                            public void configureSession(String statement) {}

                            @Override
                            public ReadableConfig getSessionConfig() throws SqlExecutionException {
                                Map<String, String> map = new HashMap<>();
                                map.put(SqlClientOptions.VERBOSE.key(), Boolean.FALSE.toString());
                                map.put(SqlClientOptions.PROMPT.key(), spec.promptPattern);
                                map.put("my_prop", "my_prop_value");
                                map.put("my_another_prop", "my_another_prop_value");
                                map.put("test", "my_prop_value");
                                return Configuration.fromMap(map);
                            }

                            @Override
                            public StatementResult executeStatement(String statement) {
                                return null;
                            }

                            @Override
                            public List<String> completeStatement(String statement, int position) {
                                return null;
                            }

                            @Override
                            public String getCurrentCatalog() {
                                return CURRENT_CATALOG;
                            }

                            @Override
                            public String getCurrentDatabase() {
                                return CURRENT_DATABASE;
                            }

                            @Override
                            public void close() {}
                        },
                        () -> TERMINAL);

        assertThat(promptHandler.getPrompt()).isEqualTo(spec.expectedPromptValue);
    }

    static Stream<Arguments> invalidPromptSupplier() {
        return Stream.of(
                of(
                        TestSpec.forPromptPattern(toAnsi("\\d"))
                                .expectedPromptValue(SqlClientOptions.PROMPT.defaultValue())),
                of(
                        TestSpec.forPromptPattern(toAnsi("\\c"))
                                .expectedPromptValue(SqlClientOptions.PROMPT.defaultValue())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidPromptSupplier")
    void promptTestForInvalidInput(TestSpec spec) {
        try {
            PromptHandler promptHandler =
                    new PromptHandler(
                            new Executor() {
                                @Override
                                public void configureSession(String statement) {}

                                @Override
                                public StatementResult executeStatement(String statement) {
                                    return null;
                                }

                                @Override
                                public String getCurrentCatalog() {
                                    return CURRENT_CATALOG;
                                }

                                @Override
                                public String getCurrentDatabase() {
                                    return CURRENT_DATABASE;
                                }

                                @Override
                                public void close() {}

                                @Override
                                public ReadableConfig getSessionConfig()
                                        throws SqlExecutionException {
                                    return new ReadableConfig() {
                                        @Override
                                        public <T> T get(ConfigOption<T> option) {
                                            if (SqlClientOptions.VERBOSE
                                                    .key()
                                                    .equals(option.key())) {
                                                return (T) Boolean.FALSE;
                                            }
                                            return null;
                                        }

                                        @Override
                                        public <T> Optional<T> getOptional(ConfigOption<T> option) {
                                            throw new UnsupportedOperationException(
                                                    "Not implemented.");
                                        }
                                    };
                                }

                                @Override
                                public List<String> completeStatement(
                                        String statement, int position) {
                                    return null;
                                }
                            },
                            () -> TERMINAL);
            assertThat(promptHandler.getPrompt())
                    .as(SqlClientOptions.PROMPT.defaultValue())
                    .isEqualTo(SqlClientOptions.PROMPT.defaultValue());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    // ------------------------------------------------------------------------------------------

    static class TestSpec {
        String expectedPromptValue;

        final String promptPattern;

        String auxiliaryPropertyKey;

        String auxiliaryPropertyValue;

        private TestSpec(String promptPattern) {
            this.promptPattern = promptPattern;
        }

        static TestSpec forPromptPattern(String promptPattern) {
            return new TestSpec(promptPattern);
        }

        TestSpec expectedPromptValue(String expectedPromptValue) {
            this.expectedPromptValue = expectedPromptValue;
            return this;
        }

        TestSpec propertyKey(String propertyKey) {
            this.auxiliaryPropertyKey = propertyKey;
            return this;
        }

        TestSpec propertyValue(String propertyValue) {
            this.auxiliaryPropertyValue = propertyValue;
            return this;
        }

        public String toString() {
            return "Expected: "
                    + expectedPromptValue
                    + ", pattern: "
                    + promptPattern
                    + ", propertyKey: "
                    + auxiliaryPropertyKey
                    + ", propertyValue: "
                    + auxiliaryPropertyValue;
        }
    }

    private static Arguments params(String expectedAnsi, String promptPattern) {
        return params(expectedAnsi, promptPattern, null, null);
    }

    private static Arguments params(
            String expectedAnsi, String promptPattern, String propName, String propValue) {
        return of(expectedAnsi, promptPattern, propName, propValue);
    }

    private static String toAnsi(String input) {
        return toAnsi(of(input, DEFAULT));
    }

    private static String toAnsi(Pair<String, AttributedStyle> pair) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append(pair.getLeft(), pair.getRight());
        return builder.toAnsi();
    }

    private static String toAnsi(
            Pair<String, AttributedStyle> pair1, Pair<String, AttributedStyle> pair2) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append(pair1.getLeft(), pair1.getRight());
        builder.append(pair2.getLeft(), pair2.getRight());
        return builder.toAnsi();
    }
}
