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
import org.apache.commons.lang3.tuple.Pair;
import org.jline.reader.EOFError;
import org.jline.reader.impl.DefaultParser;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Multi-line parser for parsing an arbitrary number of SQL lines until a line ends with ';'.
 *
 * <p>Escaping is disabled for now.
 */
public class SqlMultiLineParser extends DefaultParser {

    private static final String NEW_LINE_PROMPT = " ;"; // results in simple '>' output

    private static final String MINUS_ONE_LINE_COMMENT = "--";
    private static final String SLASH_ONE_LINE_COMMENT = "//";
    private static final String MULTILINE_COMMENT_START = "/*";
    private static final String MULTILINE_COMMENT_END = "*/";
    private static final String HINT_START = "/*+";
    private static final String SQL_IDENTIFIER = "`";
    private static final String SINGLE_QUOTE = "'";

    private final Supplier<Boolean> showHintSupplier;

    public SqlMultiLineParser() {
        this(() -> false);
    }

    public SqlMultiLineParser(Supplier<Boolean> showHintSupplier) {
        this.showHintSupplier = showHintSupplier;
    }

    @Override
    public SqlArgumentList parse(String line, int cursor, ParseContext context) {
        return parse(line, cursor, context, false);
    }

    // This is a modified version of org.jline.reader.impl.DefaultParser#parse
    public SqlArgumentList parse(
            String line, int cursor, ParseContext context, boolean respectBlankWords) {
        List<String> words = new LinkedList<>();
        StringBuilder currentWord = new StringBuilder();
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
                // the position in the currentWord argument is just the
                // length of the currentWord argument
                wordCursor = currentWord.length();
                rawWordCursor = i - rawWordStart;
            }

            Pair<String, TokenType> tokenTypePair = getTokenTypePair(line, i);
            TokenType tokenType = tokenTypePair.getRight();
            final char c = line.charAt(i);
            switch (state[i]) {
                case DEFAULT:
                    switch (tokenType) {
                        case SINGLE_QUOTE:
                        case SQL_IDENTIFIER:
                        case HINT_START:
                            state[i] = tokenType.nextState(state[i]);
                            state[i + 1] = state[i];
                            currentWord.append(c);
                            break;
                        case MULTILINE_COMMENT_START:
                        case MINUS_ONE_LINE_COMMENT_START:
                        case SLASH_LINE_COMMENT_START:
                            if (respectBlankWords
                                    && tokenType == TokenType.MULTILINE_COMMENT_START) {
                                state[i] = tokenType.nextState(state[i]);
                                state[i + 1] = state[i];
                                currentWord.append(c);
                                break;
                            }
                            handleWord(words, currentWord, state, i);
                            state[i] = tokenType.nextState(state[i]);
                            final int commentSignLength = tokenTypePair.getLeft().length();
                            int iOldValue = i;
                            i += commentSignLength - 1;
                            Arrays.fill(
                                    state,
                                    iOldValue,
                                    Math.min(i + 2, state.length),
                                    state[iOldValue]);
                            break;
                        default:
                            if (Character.isWhitespace(c)) {
                                if (currentWord.length() > 0) {
                                    handleWord(words, currentWord, state, i);
                                    if (rawWordCursor >= 0 && rawWordLength < 0) {
                                        rawWordLength = i - rawWordStart;
                                    }
                                }
                                if (respectBlankWords) {
                                    words.add(c + "");
                                }
                                rawWordStart = i + 1;
                            } else {
                                currentWord.append(line.charAt(i));
                                bracketChecker.check(line, i);
                            }
                    }
                    break;
                case SINGLE_QUOTED:
                case SQL_IDENTIFIER_QUOTED:
                    state[i + 1] = tokenType.nextState(state[i]);
                    currentWord.append(c);
                    break;

                case HINT:
                    if (tokenType == TokenType.MULTILINE_COMMENT_END) {
                        // Hints and block comments have the same ending
                        state[i + 1] = TokenType.HINT_END.nextState(state[i]);
                        currentWord.append(MULTILINE_COMMENT_END);
                        int iPrevValue = i;
                        i += MULTILINE_COMMENT_END.length() - 1;
                        Arrays.fill(
                                state,
                                iPrevValue,
                                Math.min(i + 1, state.length),
                                state[iPrevValue]);
                    } else {
                        state[i + 1] = tokenType.nextState(state[i]);
                        currentWord.append(c);
                    }
                    break;

                case MULTILINE_COMMENTED:
                case ONE_LINE_COMMENTED:
                    state[i + 1] = tokenType.nextState(state[i]);
                    if (state[i + 1] == State.DEFAULT) {
                        if (respectBlankWords && tokenType == TokenType.MULTILINE_COMMENT_END) {
                            currentWord.append(MULTILINE_COMMENT_END);
                        }
                        final int commentSignLength =
                                (tokenType == TokenType.MULTILINE_COMMENT_END
                                                ? MULTILINE_COMMENT_END
                                                : System.lineSeparator())
                                        .length();
                        int iPrevValue = i;
                        i += commentSignLength - 1;
                        Arrays.fill(
                                state,
                                iPrevValue,
                                Math.min(i + 1, state.length),
                                state[iPrevValue]);
                        rawWordStart = i + 1;
                    } else if (respectBlankWords && state[i] == State.MULTILINE_COMMENTED) {
                        currentWord.append(c);
                    }
            }
        }

        if (currentWord.length() > 0 || (line != null && cursor == line.length())) {
            handleWord(words, currentWord, state, line.length());
            if (rawWordCursor >= 0 && rawWordLength < 0) {
                rawWordLength = line.length() - rawWordStart;
            }
        }

        if (line != null && cursor == line.length()) {
            wordIndex = 0;
            wordCursor = 0;
            if (!words.isEmpty()) {
                wordIndex = words.size() - 1;
                wordCursor = words.get(wordIndex).length();
                boolean whitespace = Character.isWhitespace(line.charAt(line.length() - 1));
                if (whitespace) {
                    wordIndex = wordIndex + 1;
                    wordCursor = 0;
                }
            }

            rawWordCursor = cursor - rawWordStart;
            rawWordLength = rawWordCursor;
        }

        if (context != ParseContext.COMPLETE) {
            if (bracketChecker.isClosingBracketMissing()
                    || bracketChecker.isOpeningBracketMissing()) {
                if (bracketChecker.isClosingBracketMissing()) {
                    throw getEOFError(
                            "Missing closing bracket " + bracketChecker.getMissedClosingBracket(),
                            " " + bracketChecker.getMissedClosingBracket());
                } else {
                    throw getEOFError(
                            "Missing opening bracket " + bracketChecker.getMissingOpeningBracket(),
                            "missed " + bracketChecker.getMissingOpeningBracket());
                }
            }

            switch (state[state.length - 1]) {
                case SINGLE_QUOTED:
                    throw getEOFError("Missing closing quote '", " '");
                case SQL_IDENTIFIER_QUOTED:
                    throw getEOFError("Missing sql identifier quote `", " `");
                case HINT:
                case MULTILINE_COMMENTED:
                    throw getEOFError("Missing end of multiline comment */", "*/");
                case DEFAULT:
                case ONE_LINE_COMMENTED:
                    if (!respectBlankWords
                            && line.trim().length() > 0
                            && !line.trim().endsWith(";")) {
                        throw getEOFError("Missing semicolon", NEW_LINE_PROMPT);
                    }
                    for (int i = words.size() - 1; i >= 0; i--) {
                        String trimmed = words.get(i).trim();
                        if (!trimmed.isEmpty()) {
                            if (trimmed.endsWith(";")) {
                                break;
                            } else {
                                throw getEOFError("Missing semicolon", NEW_LINE_PROMPT);
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

    public static String getParsedCommentFreeLine(String line, Supplier<Boolean> showHintSupplier) {
        return getParsedCommentFreeLine(line, showHintSupplier, true);
    }

    public static String getParsedCommentFreeLine(
            String line, Supplier<Boolean> showHintSupplier, boolean keepBlankWords) {
        List<String> words =
                new SqlMultiLineParser(showHintSupplier)
                        .parse(line, 0, ParseContext.COMPLETE, keepBlankWords)
                        .words();
        if (keepBlankWords) {
            return String.join("", words).trim();
        } else {
            return words.stream()
                    .filter(w -> !w.trim().isEmpty())
                    .collect(Collectors.joining(" "))
                    .trim();
        }
    }

    /** The result of delimited buffer containing {@link State} for each position. */
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

    /** State describing current position. */
    enum State {
        DEFAULT,
        KEYWORD, // TODO: currently not used, should be after FLINK-24910 completed
        SINGLE_QUOTED,
        SQL_IDENTIFIER_QUOTED,
        HINT,
        ONE_LINE_COMMENTED,
        MULTILINE_COMMENTED;
    }

    private enum TokenType {
        DEFAULT(state -> state),
        SINGLE_QUOTE(TokenType::fromSingleQuote),
        SQL_IDENTIFIER(TokenType::fromSqlIdentifier),
        MINUS_ONE_LINE_COMMENT_START(
                state -> state == State.DEFAULT ? State.ONE_LINE_COMMENTED : state),
        SLASH_LINE_COMMENT_START(
                state -> state == State.DEFAULT ? State.ONE_LINE_COMMENTED : state),
        ONE_LINE_COMMENT_END(state -> state == State.ONE_LINE_COMMENTED ? State.DEFAULT : state),
        HINT_START(state -> state == State.DEFAULT ? State.HINT : state),
        HINT_END(state -> state == State.HINT ? State.DEFAULT : state),
        MULTILINE_COMMENT_START(
                state -> state == State.DEFAULT ? State.MULTILINE_COMMENTED : state),
        MULTILINE_COMMENT_END(state -> state == State.MULTILINE_COMMENTED ? State.DEFAULT : state);

        private final Function<State, State> next;

        TokenType(Function<State, State> next) {
            this.next = next;
        }

        public State nextState(State currentState) {
            return next.apply(currentState);
        }

        private static State fromSingleQuote(State state) {
            if (state == State.DEFAULT) {
                return State.SINGLE_QUOTED;
            }
            return state == State.SINGLE_QUOTED ? State.DEFAULT : state;
        }

        private static State fromSqlIdentifier(State state) {
            if (state == State.DEFAULT) {
                return State.SQL_IDENTIFIER_QUOTED;
            }
            return state == State.SQL_IDENTIFIER_QUOTED ? State.DEFAULT : state;
        }
    }

    private void handleWord(
            List<String> words, StringBuilder currentWord, State[] states, int pos) {
        if (currentWord.length() == 0) {
            return;
        }
        String word = currentWord.toString();
        if (SqlAbstractParserImpl.getSql92ReservedWords().contains(word.toUpperCase(Locale.ROOT))) {
            Arrays.fill(states, pos - word.length(), pos, State.KEYWORD);
        }
        words.add(word);
        currentWord.setLength(0);
    }

    private Pair<String, TokenType> getTokenTypePair(String line, int pos) {
        if (isMatched(SINGLE_QUOTE, line, pos)) {
            return Pair.of(SINGLE_QUOTE, TokenType.SINGLE_QUOTE);
        }
        if (isMatched(SQL_IDENTIFIER, line, pos)) {
            return Pair.of(SQL_IDENTIFIER, TokenType.SQL_IDENTIFIER);
        }
        if (isMatched(MINUS_ONE_LINE_COMMENT, line, pos)) {
            return Pair.of(MINUS_ONE_LINE_COMMENT, TokenType.MINUS_ONE_LINE_COMMENT_START);
        }
        if (isMatched(SLASH_ONE_LINE_COMMENT, line, pos)) {
            return Pair.of(SLASH_ONE_LINE_COMMENT, TokenType.SLASH_LINE_COMMENT_START);
        }
        if (isMatched(System.lineSeparator(), line, pos)) {
            return Pair.of(System.lineSeparator(), TokenType.ONE_LINE_COMMENT_END);
        }
        if (isMatched(HINT_START, line, pos)) {
            return Pair.of(HINT_START, TokenType.HINT_START);
        }
        if (isMatched(MULTILINE_COMMENT_START, line, pos)) {
            return Pair.of(MULTILINE_COMMENT_START, TokenType.MULTILINE_COMMENT_START);
        }
        if (isMatched(MULTILINE_COMMENT_END, line, pos)) {
            return Pair.of(MULTILINE_COMMENT_END, TokenType.MULTILINE_COMMENT_END);
        }
        return Pair.of(line.substring(pos), TokenType.DEFAULT);
    }

    private EOFError getEOFError(String message, String missing) {
        return new EOFError(-1, -1, message, showHintSupplier.get() ? missing : "");
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

        public Character getMissingOpeningBracket() {
            return missingOpeningBracket;
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
