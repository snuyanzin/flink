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

import org.jline.reader.EOFError;
import org.jline.reader.impl.DefaultParser;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Multi-line parser for parsing an arbitrary number of SQL lines until a line ends with ';'.
 *
 * <p>Escaping is disabled for now.
 *
 * <p>The following table shows the prompts a man may see.
 *
 * <table>
 * <caption>FlinkSQL continuation prompts</caption>
 * <tr>
 *   <th>Prompt</th>
 *   <th>Meaning</th>
 * </tr>
 * <tr>
 *   <td>Flink SQL&gt;</td>
 *   <td>Ready for a new query</td>
 * </tr>
 * <tr>
 *   <td>&gt;</td>
 *   <td>Waiting for next line of multiple-line query,
 *       waiting for completion of query with semicolon (;)</td>
 * </tr>
 * <tr>
 *   <td>'&gt;</td>
 *   <td>Waiting for next line,
 *       waiting for completion of a string that began with a single quote (')</td>
 * </tr>
 * <tr>
 *   <td>`&gt;</td>
 *   <td>Waiting for next line,
 *       waiting for completion of a string that began with a back tick (`)</td>
 * </tr>
 * <tr>
 *   <td>*\/&gt;</td>
 *   <td>Waiting for next line,
 *       waiting for completion of a multi-line comment that began with "/*" </td>
 * </tr>
 * <tr>
 *   <td>)&gt;</td>
 *   <td>Waiting for next line,
 *       waiting for completion of a string that began with a round bracket, "("</td>
 * </tr>
 * <tr>
 *   <td>]&gt;</td>
 *   <td>Waiting for next line,
 *       waiting for completion of a string that began with a square bracket, "["</td>
 * </tr>
 * <tr>
 *   <td>extra ')'&gt;</td>
 *   <td>There is an extra round bracket ")", that is not opened with "("</td>
 * </tr>
 * <tr>
 *   <td>extra ']'&gt;</td>
 *   <td>There is an extra square bracket "]", that is not opened with "["</td>
 * </tr>
 * </table>
 */
public class SqlMultiLineParser extends DefaultParser {

    private static final String NEW_LINE_PROMPT = ""; // results in simple '>' output

    private static final String MINUS_ONE_LINE_COMMENT = "--";
    private static final String SLASH_ONE_LINE_COMMENT = "//";
    private static final String MULTILINE_COMMENT_START = "/*";
    private static final String MULTILINE_COMMENT_END = "*/";
    private static final String HINT_START = "/*+";
    private static final String SQL_IDENTIFIER = "`";
    private static final String SINGLE_QUOTE = "'";

    @Override
    public SqlArgumentList parse(String line, int cursor, ParseContext context) {
        List<String> words = new LinkedList<>();
        StringBuilder current = new StringBuilder();
        BracketChecker bracketChecker = new BracketChecker();
        int wordCursor = -1;
        int wordIndex = -1;
        int rawWordCursor = -1;
        int rawWordLength = -1;
        int rawWordStart = 0;
        State[] state = new State[line.length() + 1];
        Arrays.fill(state, State.DEFAULT);
        for (int i = 0; (line != null) && (i < line.length()); i++) {
            // once we reach the cursor, set the
            // position of the selected index
            if (i == cursor) {
                wordIndex = words.size();
                // the position in the current argument is just the
                // length of the current argument
                wordCursor = current.length();
                rawWordCursor = i - rawWordStart;
            }

            TokenType tokenType = getTokenType(line, i);
            final char c = line.charAt(i);
            switch (state[i]) {
                case DEFAULT:
                    switch (tokenType) {
                        case SINGLE_QUOTE:
                        case SQL_IDENTIFIER:
                        case HINT_START:
                            state[i] = tokenType.nextState(state[i]);
                            state[i + 1] = state[i];
                            current.append(c);
                            break;
                        case MULTILINE_COMMENT_START:
                        case MINUS_ONE_LINE_COMMENT_START:
                        case SLASH_LINE_COMMENT_START:
                            updateWords(words, current);
                            state[i] = tokenType.nextState(state[i]);
                            final int length = tokenType.value.length();
                            int iOldValue = i;
                            i += length - 1;
                            for (int j = iOldValue; j <= Math.min(i + 1, state.length - 1); j++) {
                                state[j] = state[iOldValue];
                            }
                            break;
                        default:
                            if (Character.isWhitespace(c)) {
                                if (current.length() > 0) {
                                    words.add(current.toString());
                                    current.setLength(0); // reset the arg
                                    if (rawWordCursor >= 0 && rawWordLength < 0) {
                                        rawWordLength = i - rawWordStart;
                                    }
                                }
                                words.add(c + "");
                                rawWordStart = i + 1;
                            } else {
                                current.append(line.charAt(i));
                                bracketChecker.check(line, i);
                            }
                    }
                    break;
                case SINGLE_QUOTED:
                case SQL_IDENTIFIER_QUOTED:
                    state[i + 1] = tokenType.nextState(state[i]);
                    current.append(c);
                    break;

                case HINT:
                    if (tokenType == TokenType.MULTILINE_COMMENT_END) {
                        // Hints and multiline comments have the same ending
                        state[i + 1] = TokenType.HINT_END.nextState(state[i]);
                        current.append(MULTILINE_COMMENT_END);
                        int iOldValue = i;
                        i += MULTILINE_COMMENT_END.length() - 1;
                        for (int j = iOldValue; j <= Math.min(i - 1, state.length - 1); j++) {
                            state[j] = state[iOldValue];
                        }
                    } else {
                        state[i + 1] = tokenType.nextState(state[i]);
                        current.append(c);
                    }
                    break;

                case MULTILINE_COMMENTED:
                case ONE_LINE_COMMENTED:
                    state[i + 1] = tokenType.nextState(state[i]);
                    if (state[i + 1] == State.DEFAULT) {
                        final int length =
                                tokenType == TokenType.MULTILINE_COMMENT_END
                                        ? MULTILINE_COMMENT_END.length()
                                        : System.lineSeparator().length();
                        int iOldValue = i;
                        i += length - 1;
                        for (int j = iOldValue; j <= Math.min(i - 1, state.length - 1); j++) {
                            state[j] = state[iOldValue];
                        }
                        rawWordStart = i + 1;
                    }
            }
        }

        if (current.length() > 0 || (line != null && cursor == line.length())) {
            words.add(current.toString());
            if (rawWordCursor >= 0 && rawWordLength < 0) {
                rawWordLength = line.length() - rawWordStart;
            }
        }

        if (line != null && cursor == line.length()) {
            wordIndex = words.size() - 1;
            wordCursor = words.get(words.size() - 1).length();
            rawWordCursor = cursor - rawWordStart;
            rawWordLength = rawWordCursor;
        }

        if (context != ParseContext.COMPLETE) {
            if (bracketChecker.isClosingBracketMissing()
                    || bracketChecker.isOpeningBracketMissing()) {
                String message;
                String missing;
                if (bracketChecker.isClosingBracketMissing()) {
                    message = "Missing closing bracket " + bracketChecker.getMissedClosingBracket();
                    missing = "" + bracketChecker.getMissedClosingBracket();
                } else {
                    message = "Missing opening bracket " + bracketChecker.missingOpeningBracket;
                    missing = "extra " + bracketChecker.missingOpeningBracket;
                }
                throw new EOFError(-1, -1, message, missing);
            }

            switch (state[state.length - 1]) {
                case SINGLE_QUOTED:
                    throw new EOFError(-1, -1, "Missing closing quote '", "'");
                case SQL_IDENTIFIER_QUOTED:
                    throw new EOFError(-1, -1, "Missing sql identifier quote `", "`");
                case HINT:
                case MULTILINE_COMMENTED:
                    throw new EOFError(-1, -1, "Missing end of multiline comment */", "*/");
                case DEFAULT:
                case ONE_LINE_COMMENTED:
                    for (int i = words.size() - 1; i >= 0; i--) {
                        String trimmed = words.get(i).trim();
                        if (!trimmed.isEmpty()) {
                            if (trimmed.endsWith(";")) {
                                break;
                            } else {
                                throw new EOFError(-1, -1, "Missing semicolon", NEW_LINE_PROMPT);
                            }
                        }
                    }
            }
        }

        return new SqlArgumentList(
                state,
                line,
                words,
                wordIndex,
                wordCursor,
                cursor,
                null,
                rawWordCursor,
                rawWordLength);
    }

    public static String getParsedCommentFreeLine(String line) {
        return getParsedCommentFreeLine(line, true);
    }

    public static String getParsedCommentFreeLine(String line, boolean keepBlankWords) {
        List<String> words = new SqlMultiLineParser().parse(line, 0, ParseContext.COMPLETE).words();
        if (keepBlankWords) {
            return String.join("", words).trim();
        } else {
            return words.stream()
                    .filter(w -> !w.trim().isEmpty())
                    .collect(Collectors.joining(" "))
                    .trim();
        }
    }

    /** test. */
    public class SqlArgumentList extends DefaultParser.ArgumentList {

        private final State[] state;

        public SqlArgumentList(
                State[] state,
                String line,
                List<String> words,
                int wordIndex,
                int wordCursor,
                int cursor,
                String openingQuote,
                int rawWordCursor,
                int rawWordLength) {

            super(
                    line,
                    words,
                    wordIndex,
                    wordCursor,
                    cursor,
                    openingQuote,
                    rawWordCursor,
                    rawWordLength);
            this.state = state;
        }

        public State[] getState() {
            return state;
        }

        @Override
        public CharSequence escape(CharSequence candidate, boolean complete) {
            // escaping is skipped for now because it highly depends on the context in the SQL
            // statement
            // e.g. 'INS(ERT INTO)' should not be quoted but 'SELECT * FROM T(axi Rides)' should
            return candidate;
        }
    }

    // ------------------------------------------------------------------------------------------------

    /** State describing cursor position. */
    public enum State {
        DEFAULT,
        SINGLE_QUOTED,
        SQL_IDENTIFIER_QUOTED,
        HINT,
        ONE_LINE_COMMENTED,
        MULTILINE_COMMENTED;
    }

    private enum TokenType {
        DEFAULT(null, state -> state),
        SINGLE_QUOTE(SqlMultiLineParser.SINGLE_QUOTE, TokenType::fromSingleQuote),
        SQL_IDENTIFIER(SqlMultiLineParser.SQL_IDENTIFIER, TokenType::fromSqlIdentifier),
        MINUS_ONE_LINE_COMMENT_START(MINUS_ONE_LINE_COMMENT, TokenType::fromOneLineCommentStart),
        SLASH_LINE_COMMENT_START(SLASH_ONE_LINE_COMMENT, TokenType::fromOneLineCommentStart),
        ONE_LINE_COMMENT_END(System.lineSeparator(), TokenType::fromOneLineCommentEnd),
        HINT_START(SqlMultiLineParser.HINT_START, TokenType::fromHintStart),
        HINT_END(SqlMultiLineParser.MULTILINE_COMMENT_END, TokenType::fromHintEnd),
        MULTILINE_COMMENT_START(
                SqlMultiLineParser.MULTILINE_COMMENT_START, TokenType::fromMultilineCommentStart),
        MULTILINE_COMMENT_END(
                SqlMultiLineParser.MULTILINE_COMMENT_END, TokenType::fromMultilineCommentEnd);

        private final Function<State, State> next;
        private final String value;

        TokenType(String value, Function<State, State> next) {
            this.next = next;
            this.value = value;
        }

        public State nextState(State currentState) {
            return next.apply(currentState);
        }

        private static State getDefaultIfEquals(State state1, State state2) {
            if (state1 == state2) {
                return State.DEFAULT;
            }
            return state1;
        }

        private static State getIfDefault(State ifDefault, State ifNotDefault) {
            if (ifNotDefault == State.DEFAULT) {
                return ifDefault;
            }
            return ifNotDefault;
        }

        private static State fromSingleQuote(State state) {
            if (state == State.DEFAULT) {
                return State.SINGLE_QUOTED;
            } else if (state == State.SINGLE_QUOTED) {
                return State.DEFAULT;
            }
            return state;
        }

        private static State fromSqlIdentifier(State state) {
            if (state == State.DEFAULT) {
                return State.SQL_IDENTIFIER_QUOTED;
            } else if (state == State.SQL_IDENTIFIER_QUOTED) {
                return State.DEFAULT;
            }
            return state;
        }

        private static State fromOneLineCommentEnd(State state) {
            return getDefaultIfEquals(state, State.ONE_LINE_COMMENTED);
        }

        private static State fromOneLineCommentStart(State state) {
            return getIfDefault(State.ONE_LINE_COMMENTED, state);
        }

        private static State fromHintEnd(State state) {
            return getDefaultIfEquals(state, State.HINT);
        }

        private static State fromHintStart(State state) {
            return getIfDefault(State.HINT, state);
        }

        private static State fromMultilineCommentEnd(State state) {
            return getDefaultIfEquals(state, State.MULTILINE_COMMENTED);
        }

        private static State fromMultilineCommentStart(State state) {
            return getIfDefault(State.MULTILINE_COMMENTED, state);
        }
    }

    private void updateWords(List<String> words, StringBuilder current) {
        if (current.length() > 0) {
            words.add(current.toString());
            current.setLength(0);
        }
    }

    private TokenType getTokenType(String line, int pos) {
        if (isMatched(SINGLE_QUOTE, line, pos)) {
            return TokenType.SINGLE_QUOTE;
        }
        if (isMatched(SQL_IDENTIFIER, line, pos)) {
            return TokenType.SQL_IDENTIFIER;
        }
        if (isMatched(MINUS_ONE_LINE_COMMENT, line, pos)) {
            return TokenType.MINUS_ONE_LINE_COMMENT_START;
        }
        if (isMatched(SLASH_ONE_LINE_COMMENT, line, pos)) {
            return TokenType.SLASH_LINE_COMMENT_START;
        }
        if (isMatched(System.lineSeparator(), line, pos)) {
            return TokenType.ONE_LINE_COMMENT_END;
        }
        if (isMatched(HINT_START, line, pos)) {
            return TokenType.HINT_START;
        }
        if (isMatched(MULTILINE_COMMENT_START, line, pos)) {
            return TokenType.MULTILINE_COMMENT_START;
        }
        if (isMatched(MULTILINE_COMMENT_END, line, pos)) {
            return TokenType.MULTILINE_COMMENT_END;
        }
        return TokenType.DEFAULT;
    }

    private static boolean isMatched(String line2check, String line, int pos) {
        return line2check.regionMatches(0, line, pos, line2check.length());
    }

    /**
     * A checker for brackets inside a query. The idea is based on
     * org.jline.reader.impl.DefaultParser.BracketChecker
     */
    private static class BracketChecker {
        private static final String OPENING_BRACKETS = "([";
        private static final String CLOSING_BRACKETS = ")]";
        private Character missingOpeningBracket = null;
        private final Deque<Character> nested = new ArrayDeque<>();

        public void check(final String line, final int pos) {
            if (pos < 0) {
                return;
            }
            Character bid = bracket(OPENING_BRACKETS, line, pos);
            if (bid != null) {
                nested.addFirst(bid);
            } else {
                bid = bracket(CLOSING_BRACKETS, line, pos);
                if (bid != null) {
                    int openBracketIndex = CLOSING_BRACKETS.indexOf(bid);
                    assert openBracketIndex >= 0;
                    char openBracket = OPENING_BRACKETS.charAt(openBracketIndex);
                    if (!nested.isEmpty() && openBracket == nested.peekFirst()) {
                        nested.pop();
                    } else {
                        missingOpeningBracket = openBracket;
                    }
                }
            }
        }

        public boolean isOpeningBracketMissing() {
            return missingOpeningBracket != null;
        }

        public boolean isClosingBracketMissing() {
            return !nested.isEmpty();
        }

        public Character getMissedClosingBracket() {
            if (nested.isEmpty()) {
                return null;
            }
            int closeBracketIndex = OPENING_BRACKETS.indexOf(nested.peek());
            assert closeBracketIndex >= 0;
            return CLOSING_BRACKETS.charAt(closeBracketIndex);
        }

        private Character bracket(final String brackets, final String line, final int pos) {
            for (int i = 0; i < brackets.length(); i++) {
                if (line.charAt(pos) == brackets.charAt(i)) {
                    return line.charAt(pos);
                }
            }
            return null;
        }
    }
}
