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

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.reader.Parser;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.Locale;

/** etst. */
public class SqlHighlighter implements Highlighter {
    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        SqlMultiLineParser.SqlArgumentList list =
                new SqlMultiLineParser().parse(buffer, 0, Parser.ParseContext.COMPLETE);
        final AttributedStringBuilder sb = new AttributedStringBuilder();
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < buffer.length(); i++) {
            switch (list.getState()[i]) {
                case SINGLE_QUOTED:
                    s.setLength(0);
                    sb.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
                    sb.append(buffer.charAt(i));
                    break;
                case SQL_IDENTIFIER_QUOTED:
                    s.setLength(0);
                    sb.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW));
                    sb.append(buffer.charAt(i));
                    break;
                case ONE_LINE_COMMENTED:
                    s.setLength(0);
                    sb.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE));
                    sb.append(buffer.charAt(i));
                    break;
                case MULTILINE_COMMENTED:
                    s.setLength(0);
                    sb.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN));
                    sb.append(buffer.charAt(i));
                    break;
                case DEFAULT:
                    sb.style(AttributedStyle.DEFAULT);
                    while (i < buffer.length()
                            && list.getState()[i] == SqlMultiLineParser.State.DEFAULT) {
                        StringBuilder word = new StringBuilder();
                        while (i < buffer.length()
                                && list.getState()[i] == SqlMultiLineParser.State.DEFAULT
                                && (Character.isLetter(buffer.charAt(i))
                                        || buffer.charAt(i) == '_')) {
                            word.append(buffer.charAt(i));
                            i++;
                        }
                        if (SqlAbstractParserImpl.getSql92ReservedWords()
                                .contains(word.toString().toUpperCase(Locale.ROOT))) {
                            sb.style(
                                    AttributedStyle.DEFAULT
                                            .bold()
                                            .foreground(AttributedStyle.BLUE));
                        }
                        sb.append(word);
                        while (i < buffer.length()
                                && list.getState()[i] == SqlMultiLineParser.State.DEFAULT
                                && (!Character.isLetter(buffer.charAt(i))
                                        && buffer.charAt(i) != '_')) {
                            sb.style(AttributedStyle.DEFAULT).append(buffer.charAt(i));
                            i++;
                        }
                    }

                    handleKeyWords(s.toString(), sb, 0);
                    break;
                default:
                    sb.style(AttributedStyle.DEFAULT);
            }
        }
        return sb.toAttributedString();
    }

    private void handleKeyWords(
            String string, AttributedStringBuilder currentStringBuilder, int startPos) {
        if (string.length() <= startPos) {
            return;
        }
        int i = startPos;
        while (i < string.length()
                && !Character.isLetter(string.charAt(i))
                && string.charAt(i) != '_') {
            currentStringBuilder.style(AttributedStyle.DEFAULT).append(string.charAt(i++));
        }
        int start = i;
        while (i < string.length()
                && (Character.isLetter(string.charAt(i)) || string.charAt(i) == '_')) {
            i++;
        }
        String candidate =
                i == string.length() ? string.substring(start) : string.substring(start, i);
        if (SqlAbstractParserImpl.getSql92ReservedWords()
                .contains(candidate.toUpperCase(Locale.ROOT))) {
            currentStringBuilder.style(
                    AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.BLUE));
        }
        currentStringBuilder.append(candidate);
        handleKeyWords(string, currentStringBuilder, i);
    }
}
