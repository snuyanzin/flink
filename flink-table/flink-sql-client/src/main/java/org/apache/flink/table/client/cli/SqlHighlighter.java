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

import org.jline.reader.LineReader;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.WCWidth;

/** etst. */
public class SqlHighlighter extends DefaultHighlighter {
    private final String sessionId;
    private final Executor executor;

    private class UnderlineMetrics {
        private int underlineStart = -1;
        private int underlineEnd = -1;
        private int negativeStart = -1;
        private int negativeEnd = -1;

        public int getUnderlineStart() {
            return underlineStart;
        }

        public void setUnderlineStart(int underlineStart) {
            this.underlineStart = underlineStart;
        }

        public int getUnderlineEnd() {
            return underlineEnd;
        }

        public void setUnderlineEnd(int underlineEnd) {
            this.underlineEnd = underlineEnd;
        }

        public int getNegativeStart() {
            return negativeStart;
        }

        public void setNegativeStart(int negativeStart) {
            this.negativeStart = negativeStart;
        }

        public int getNegativeEnd() {
            return negativeEnd;
        }

        public void setNegativeEnd(int negativeEnd) {
            this.negativeEnd = negativeEnd;
        }
    }

    public SqlHighlighter(String sessionId, Executor executor) {
        super();
        this.executor = executor;
        this.sessionId = sessionId;
    }

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        SyntaxHighlightStyle style =
                executor.getSessionConfig(sessionId)
                        .get(SqlClientOptions.SYNTAX_HIGHLIGHT_STYLE)
                        .getHighlightStyle();
        if (style == SyntaxHighlightStyle.BuiltInStyle.DEFAULT.getHighlightStyle()) {
            return super.highlight(reader, buffer);
        }
        SqlMultiLineParser.SqlArgumentList list =
                new SqlMultiLineParser().parse(buffer, 0, Parser.ParseContext.COMPLETE);
        final AttributedStringBuilder sb = new AttributedStringBuilder();
        int underlineStart = -1;
        int underlineEnd = -1;
        int negativeStart = -1;
        int negativeEnd = -1;
        String search = reader.getSearchTerm();
        if (search != null && search.length() > 0) {
            underlineStart = buffer.indexOf(search);
            if (underlineStart >= 0) {
                underlineEnd = underlineStart + search.length() - 1;
            }
        }
        if (reader.getRegionActive() != LineReader.RegionType.NONE) {
            negativeStart = reader.getRegionMark();
            negativeEnd = reader.getBuffer().cursor();
            if (negativeStart > negativeEnd) {
                int x = negativeEnd;
                negativeEnd = negativeStart;
                negativeStart = x;
            }
            if (reader.getRegionActive() == LineReader.RegionType.LINE) {
                while (negativeStart > 0 && reader.getBuffer().atChar(negativeStart - 1) != '\n') {
                    negativeStart--;
                }
                while (negativeEnd < reader.getBuffer().length() - 1
                        && reader.getBuffer().atChar(negativeEnd + 1) != '\n') {
                    negativeEnd++;
                }
            }
        }

        for (int i = 0; i < buffer.length(); i++) {
            switch (list.getState()[i]) {
                case SINGLE_QUOTED:
                    sb.style(style.getQuotedStyle());
                    break;
                case SQL_IDENTIFIER_QUOTED:
                    sb.style(style.getSqlIdentifierStyle());
                    break;
                case ONE_LINE_COMMENTED:
                    sb.style(style.getLineCommentStyle());
                    break;
                case MULTILINE_COMMENTED:
                    sb.style(style.getBlockCommentStyle());
                    break;
                case KEYWORD:
                    sb.style(style.getKeywordStyle());
                    break;
                case HINT:
                    sb.style(style.getHintStyle());
                    break;
                case DEFAULT:
                    sb.style(style.getDefaultStyle());
                    break;
                default:
                    sb.style(style.getDefaultStyle());
            }

            if (i == underlineStart) {
                sb.style(AttributedStyle::underline);
            }
            if (i == negativeStart) {
                sb.style(AttributedStyle::inverse);
            }
            if (i == errorIndex) {
                sb.style(AttributedStyle::inverse);
            }

            char c = buffer.charAt(i);
            if (c == '\t' || c == '\n') {
                sb.append(c);
            } else if (c < 32) {
                sb.style(AttributedStyle::inverseNeg)
                        .append('^')
                        .append((char) (c + '@'))
                        .style(AttributedStyle::inverseNeg);
            } else {
                int w = WCWidth.wcwidth(c);
                if (w > 0) {
                    sb.append(c);
                }
            }
            if (i == underlineEnd) {
                sb.style(AttributedStyle::underlineOff);
            }
            if (i == negativeEnd) {
                sb.style(AttributedStyle::inverseOff);
            }
            if (i == errorIndex) {
                sb.style(AttributedStyle::inverseOff);
            }
        }
        if (errorPattern != null) {
            sb.styleMatches(errorPattern, AttributedStyle.INVERSE);
        }
        return sb.toAttributedString();
    }

    private void append(String buffer, AttributedStringBuilder sb, UnderlineMetrics um, int i) {
        if (i >= um.getUnderlineStart() && i <= um.getUnderlineEnd()) {
            sb.style(sb.style().underline());
        }
        if (i >= um.getNegativeStart() && i <= um.getNegativeEnd()) {
            sb.style(sb.style().inverse());
        }
        char c = buffer.charAt(i);
        if (c == '\t' || c == '\n') {
            sb.append(c);
        } else if (c < 32) {
            sb.style(AttributedStyle::inverseNeg)
                    .append('^')
                    .append((char) (c + '@'))
                    .style(AttributedStyle::inverseNeg);
        } else {
            int w = WCWidth.wcwidth(c);
            if (w > 0) {
                sb.append(c);
            }
        }
    }
}
