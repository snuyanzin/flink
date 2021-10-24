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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.apache.commons.lang3.tuple.Pair;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.commons.lang3.tuple.Pair.of;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;

/** Test {@link PromptHandler}. */
@RunWith(Parameterized.class)
public class PromptHandlerTest {
    private static final String SESSION_ID = "sessionId";
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

    @Parameterized.Parameters(
            name =
                    "Expected: {0}, Prompt property value: {1}, Auxiliary property key: {2}, Auxiliary property value: {3}")
    public static Object[][] parameters() {
        return new Object[][] {
            params(toAnsi(of("", DEFAULT)), ""),
            params(toAnsi(of("simple_prompt", DEFAULT)), "simple_prompt"),
            params(toAnsi(of("", DEFAULT)), "\\"),
            params(toAnsi(of("\\", DEFAULT)), "\\\\"),
            params(toAnsi(of("\\", DEFAULT)), "\\\\\\"),
            params(toAnsi(of("\\\\", DEFAULT)), "\\\\\\\\"),
            params(toAnsi(of("", DEFAULT.foreground(RED).bold())), ""),
            params(toAnsi(of(CURRENT_DATABASE, DEFAULT)), "\\d"),
            params(toAnsi(of(CURRENT_CATALOG, DEFAULT)), "\\c"),
            params(
                    toAnsi(
                            of("\\", DEFAULT),
                            of("my prompt", DEFAULT.foreground(AttributedStyle.GREEN).underline())),
                    "\\\\\\[f:g,underline\\]my prompt"),
            // property value in prompt
            params(
                    toAnsi(of("my_prop_value>", DEFAULT)),
                    "\\:my_prop\\:>",
                    "my_prop",
                    "my_prop_value"),
            // escaping of backslash \
            params(
                    toAnsi(of("\\[b:y,italic]my_prop_value>", DEFAULT)),
                    "\\\\[b:y,italic\\]\\:test\\:\\[default\\]>",
                    "test",
                    "my_prop_value"),
            // not specified \X will be handled as X
            params(toAnsi(of("X>", DEFAULT)), "\\X>"),
            // if any of patterns \[...\], \{...\}, \:...\: not closed it will be handled as \X
            params(
                    toAnsi(of("{ my_another_prop_value>", DEFAULT)),
                    "\\{ \\:my_prop\\:\\[default\\]>",
                    "my_prop",
                    "my_another_prop_value"),
            params(
                    toAnsi(of(":my_prop:>", DEFAULT)),
                    "\\:my_prop:\\[default\\]>",
                    "my_prop",
                    "my_another_prop_value"),
            params(
                    toAnsi(of("[default]my_another_prop_value>", DEFAULT)),
                    "\\[default]\\:my_prop\\:>",
                    "my_prop",
                    "my_another_prop_value")
        };
    }

    @Parameterized.Parameter() public String expected;

    @Parameterized.Parameter(1)
    public String promptValue;

    @Parameterized.Parameter(2)
    public String auxiliaryPropertyKey;

    @Parameterized.Parameter(3)
    public String auxiliaryPropertyValue;

    @Test
    public void promptTest() {
        TestingExecutor executor =
                new TestingExecutor(Collections.emptyList(), Collections.emptyList()) {

                    @Override
                    public String getCurrentCatalogName(String sessionId) {
                        return "current_catalog";
                    }

                    @Override
                    public String getCurrentDatabaseName(String sessionId) {
                        return CURRENT_DATABASE;
                    }

                    @Override
                    public Map<String, String> getSessionConfigMap(String sessionId)
                            throws SqlExecutionException {
                        return Collections.singletonMap(
                                auxiliaryPropertyKey, auxiliaryPropertyValue);
                    }

                    @Override
                    public ReadableConfig getSessionConfig(String sessionId)
                            throws SqlExecutionException {
                        return new ReadableConfig() {
                            @Override
                            public <T> T get(ConfigOption<T> option) {
                                return (T) promptValue;
                            }

                            @Override
                            public <T> Optional<T> getOptional(ConfigOption<T> option) {
                                throw new UnsupportedOperationException("Not implemented.");
                            }
                        };
                    }
                };
        PromptHandler promptHandler = new PromptHandler(SESSION_ID, executor, () -> TERMINAL);

        Assert.assertEquals(expected, promptHandler.getPrompt());
    }

    // ------------------------------------------------------------------------------------------
    private static Object[] params(String expectedAnsi, String promptPattern) {
        return params(expectedAnsi, promptPattern, null, null);
    }

    private static Object[] params(
            String expectedAnsi, String promptPattern, String propName, String propValue) {
        return new Object[] {expectedAnsi, promptPattern, propName, propValue};
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
