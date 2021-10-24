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

import org.jline.utils.AttributedStyle;

/** test. */
public class SyntaxHighlightStyle {

    /** test. */
    public enum BuiltInStyle {
        DEFAULT(null, null, null, null, null, null, null, null),
        DARK(BOLD_BLUE, WHITE, ITALIC_GREEN, ITALIC_GREEN, BOLD_GREEN, YELLOW, RED, MAGENTA),
        LIGHT(BOLD_RED, WHITE, ITALIC_CYAN, ITALIC_CYAN, BOLD_CYAN, YELLOW, GREEN, MAGENTA);
        private final SyntaxHighlightStyle style;

        BuiltInStyle(
                AttributedStyle keywordStyle,
                AttributedStyle defaultStyle,
                AttributedStyle blockCommentStyle,
                AttributedStyle lineCommentStyle,
                AttributedStyle hintStyle,
                AttributedStyle numberStyle,
                AttributedStyle singleQuotedStyle,
                AttributedStyle sqlIdentifierStyle) {
            style =
                    new SyntaxHighlightStyle(
                            blockCommentStyle,
                            lineCommentStyle,
                            hintStyle,
                            defaultStyle,
                            keywordStyle,
                            numberStyle,
                            singleQuotedStyle,
                            sqlIdentifierStyle);
        }

        public SyntaxHighlightStyle getHighlightStyle() {
            return style;
        }

        public static SyntaxHighlightStyle.BuiltInStyle fromOrdinal(int ord) {
            ord = ord % values().length;
            for (SyntaxHighlightStyle.BuiltInStyle style : values()) {
                if (style.ordinal() == ord) {
                    return style;
                }
            }
            // should never happen
            return DEFAULT;
        }
    }

    private final AttributedStyle blockCommentStyle;
    private final AttributedStyle lineCommentStyle;
    private final AttributedStyle hintStyle;
    private final AttributedStyle defaultStyle;
    private final AttributedStyle keywordStyle;
    private final AttributedStyle numberStyle;
    private final AttributedStyle singleQuotedStyle;
    private final AttributedStyle sqlIdentifierStyle;

    public SyntaxHighlightStyle(
            AttributedStyle blockCommentStyle,
            AttributedStyle lineCommentStyle,
            AttributedStyle hintStyle,
            AttributedStyle defaultStyle,
            AttributedStyle keywordStyle,
            AttributedStyle numberStyle,
            AttributedStyle singleQuotedStyle,
            AttributedStyle sqlIdentifierStyle) {
        this.blockCommentStyle = blockCommentStyle;
        this.lineCommentStyle = lineCommentStyle;
        this.hintStyle = hintStyle;
        this.defaultStyle = defaultStyle;
        this.keywordStyle = keywordStyle;
        this.numberStyle = numberStyle;
        this.singleQuotedStyle = singleQuotedStyle;
        this.sqlIdentifierStyle = sqlIdentifierStyle;
    }

    /**
     * Returns the style for a SQL keyword such as {@code SELECT} or {@code ON}.
     *
     * @return Style for SQL keywords
     */
    public AttributedStyle getKeywordStyle() {
        return keywordStyle;
    }

    /**
     * Returns the style for a SQL character literal, such as {@code 'Hello, world!'}.
     *
     * @return Style for SQL character literals
     */
    public AttributedStyle getQuotedStyle() {
        return singleQuotedStyle;
    }

    /**
     * Returns the style for a SQL identifier, such as {@code `My_table`} or {@code `My table`}.
     *
     * @return Style for SQL identifiers
     */
    public AttributedStyle getSqlIdentifierStyle() {
        return sqlIdentifierStyle;
    }

    /**
     * Returns the style for a SQL comments, such as {@literal /* This is a comment *}{@literal /}
     * or {@literal -- End of line comment}.
     *
     * @return Style for SQL comments
     */
    public AttributedStyle getLineCommentStyle() {
        return lineCommentStyle;
    }

    public AttributedStyle getBlockCommentStyle() {
        return blockCommentStyle;
    }

    public AttributedStyle getHintStyle() {
        return hintStyle;
    }

    /**
     * Returns the style for numeric literals.
     *
     * @return Style for numeric literals
     */
    public AttributedStyle getNumberStyle() {
        return numberStyle;
    }

    /**
     * Returns the style for text that does not match any other style.
     *
     * @return Default style
     */
    public AttributedStyle getDefaultStyle() {
        return defaultStyle;
    }

    // --------------------------------------------------------------------------------------------

    static final AttributedStyle GREEN = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    static final AttributedStyle CYAN = AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN);
    static final AttributedStyle BRIGHT =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT);
    static final AttributedStyle YELLOW =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
    static final AttributedStyle WHITE = AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE);
    static final AttributedStyle BLACK = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLACK);
    static final AttributedStyle MAGENTA =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
    static final AttributedStyle RED = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);
    static final AttributedStyle BLUE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE);

    static final AttributedStyle BOLD_GREEN =
            AttributedStyle.BOLD.foreground(AttributedStyle.GREEN);
    static final AttributedStyle BOLD_CYAN = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN);
    static final AttributedStyle BOLD_BRIGHT =
            AttributedStyle.BOLD.foreground(AttributedStyle.BRIGHT);
    static final AttributedStyle BOLD_YELLOW =
            AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW);
    static final AttributedStyle BOLD_WHITE =
            AttributedStyle.BOLD.foreground(AttributedStyle.WHITE);
    static final AttributedStyle BOLD_BLACK =
            AttributedStyle.BOLD.foreground(AttributedStyle.BLACK);
    static final AttributedStyle BOLD_MAGENTA =
            AttributedStyle.BOLD.foreground(AttributedStyle.MAGENTA);
    static final AttributedStyle BOLD_RED = AttributedStyle.BOLD.foreground(AttributedStyle.RED);
    static final AttributedStyle BOLD_BLUE = AttributedStyle.BOLD.foreground(AttributedStyle.BLUE);

    static final AttributedStyle ITALIC_GREEN =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.GREEN);
    static final AttributedStyle ITALIC_CYAN =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.CYAN);
    static final AttributedStyle ITALIC_BRIGHT =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.BRIGHT);
    static final AttributedStyle ITALIC_YELLOW =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.YELLOW);
    static final AttributedStyle ITALIC_WHITE =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.WHITE);
    static final AttributedStyle ITALIC_BLACK =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.BLACK);
    static final AttributedStyle ITALIC_MAGENTA =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.MAGENTA);
    static final AttributedStyle ITALIC_RED =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.RED);
    static final AttributedStyle ITALIC_BLUE =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.BLUE);
}
