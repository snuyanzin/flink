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

import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.utils.ThreadLocalCache;

import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.StyleResolver;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Prompt handler class which allows customization for the prompt shown at the start (left prompt)
 * and the end (right prompt) of each line.
 */
public class PromptHandler {
    private static final char ESCAPE_BACKSLASH = '\\';
    private static final Map<Character, String> DATE_TIME_FORMATS;
    private static final ThreadLocalCache<String, SimpleDateFormat> FORMATTER_CACHE =
            new ThreadLocalCache<String, SimpleDateFormat>() {
                @Override
                public SimpleDateFormat getNewInstance(String key) {
                    return new SimpleDateFormat(key, Locale.ROOT);
                }
            };

    static {
        Map<Character, String> map = new HashMap<>();
        map.put('D', "yyyy-MM-dd HH:mm:ss.SSS");
        map.put('m', "mm");
        map.put('o', "MM");
        map.put('O', "MMM");
        map.put('P', "aa");
        map.put('r', "hh:mm");
        map.put('R', "HH:mm");
        map.put('s', "ss");
        map.put('w', "d");
        map.put('W', "E");
        map.put('y', "YY");
        map.put('Y', "YYYY");
        DATE_TIME_FORMATS = Collections.unmodifiableMap(map);
    }

    private static final StyleResolver STYLE_RESOLVER = new StyleResolver(s -> "");

    private final String sessionId;
    private final Executor executor;
    private final Supplier<Terminal> terminalSupplier;

    public PromptHandler(String sessionId, Executor executor, Supplier<Terminal> terminalSupplier) {
        this.sessionId = sessionId;
        this.executor = executor;
        this.terminalSupplier = terminalSupplier;
    }

    public String getPrompt() {
        return buildPrompt(
                executor.getSessionConfig(sessionId).get(SqlClientOptions.PROMPT),
                SqlClientOptions.PROMPT.defaultValue());
    }

    public String getRightPrompt() {
        return buildPrompt(
                executor.getSessionConfig(sessionId).get(SqlClientOptions.RIGHT_PROMPT),
                SqlClientOptions.RIGHT_PROMPT.defaultValue());
    }

    private String buildPrompt(String pattern, String defaultValue) {
        AttributedStringBuilder promptStringBuilder = new AttributedStringBuilder();
        try {
            for (int i = 0; i < pattern.length(); i++) {
                final char c = pattern.charAt(i);
                if (c == ESCAPE_BACKSLASH) {
                    if (i == pattern.length() - 1) {
                        continue;
                    }
                    final char nextChar = pattern.charAt(i + 1);
                    switch (nextChar) {
                        case 'D':
                        case 'm':
                        case 'o':
                        case 'O':
                        case 'P':
                        case 'r':
                        case 'R':
                        case 's':
                        case 'w':
                        case 'W':
                        case 'y':
                        case 'Y':
                            promptStringBuilder.append(
                                    FORMATTER_CACHE
                                            .get(DATE_TIME_FORMATS.get(nextChar))
                                            .format(new Date()));
                            i++;
                            break;
                        case 'c':
                            promptStringBuilder.append(executor.getCurrentCatalogName(sessionId));
                            i++;
                            break;
                        case 'd':
                            promptStringBuilder.append(executor.getCurrentDatabaseName(sessionId));
                            i++;
                            break;
                        case ESCAPE_BACKSLASH:
                            promptStringBuilder.append(ESCAPE_BACKSLASH);
                            i++;
                            break;
                            // date time pattern \{...\}
                        case '{':
                            int dateTimeMaskCloseIndex =
                                    pattern.indexOf(ESCAPE_BACKSLASH + "}", i + 1);
                            if (dateTimeMaskCloseIndex > 0) {
                                String mask = pattern.substring(i + 2, dateTimeMaskCloseIndex);
                                promptStringBuilder.append(
                                        FORMATTER_CACHE.get(mask).format(new Date()));
                                i = dateTimeMaskCloseIndex + 1;
                            }
                            break;
                            // color and style pattern \[...\]
                        case '[':
                            int closeBracketIndex = pattern.indexOf(ESCAPE_BACKSLASH + "]", i + 1);
                            if (closeBracketIndex > 0) {
                                String color = pattern.substring(i + 2, closeBracketIndex);
                                AttributedStyle style = STYLE_RESOLVER.resolve(color);
                                promptStringBuilder.style(style);
                                i = closeBracketIndex + 1;
                            }
                            break;
                            // property value pattern \:...\:
                        case ':':
                            int nextColonIndex = pattern.indexOf(ESCAPE_BACKSLASH + ":", i + 1);
                            String propertyValue;
                            if (nextColonIndex > 0) {
                                String propertyName = pattern.substring(i + 2, nextColonIndex);
                                propertyValue =
                                        executor.getSessionConfigMap(sessionId).get(propertyName);
                                promptStringBuilder.append(propertyValue);
                                i = nextColonIndex + 1;
                            }
                            break;
                    }
                } else {
                    promptStringBuilder.append(c);
                }
            }
            return promptStringBuilder.toAnsi();
        } catch (IllegalArgumentException e) {
            terminalSupplier
                    .get()
                    .writer()
                    .println(
                            CliStrings.messageError(
                                            e.getMessage(),
                                            e,
                                            executor.getSessionConfig(sessionId)
                                                    .get(SqlClientOptions.VERBOSE))
                                    .toAnsi());
            return defaultValue;
        }
    }
}
