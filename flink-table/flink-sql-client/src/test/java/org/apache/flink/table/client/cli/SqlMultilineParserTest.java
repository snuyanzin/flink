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

import org.apache.commons.lang3.tuple.Pair;
import org.jline.reader.Parser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/** Test {@link SqlMultiLineParser}. */
@RunWith(Parameterized.class)
public class SqlMultilineParserTest {

    private static final SqlMultiLineParser PARSER = new SqlMultiLineParser();

    @Parameterized.Parameters(
            name = "Parsing: {0}. Expected identifier: {1}, Expected exception message: {2}")
    public static Object[][] parameters() {
        return new Object[][] {
            // valid
            ok(
                    "'select',",
                    "'select',",
                    new String2StateConverter().appendQuoted("'select'").append(",").build()),
            ok(
                    "select'test';",
                    "select'test';",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .appendQuoted("'test'")
                            .append(";")
                            .build()),
            ok("select", "select", new String2StateConverter().appendKeyWord("select").build()),
            ok(
                    "select(current_date);",
                    "select(current_date);",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append("(")
                            .appendKeyWord("current_date")
                            .append(");")
                            .build()),
            ok(
                    "select /*+ hint */ 1;",
                    "select /*+ hint */ 1;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendHint("/*+ hint */")
                            .append(" 1;")
                            .build()),
            ok(
                    "select max (2)",
                    "select max (2)",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendKeyWord("max")
                            .append(" (2)")
                            .build()),
            ok(
                    "--select 1;",
                    "",
                    new String2StateConverter()
                            .appendLineComment("--select 1;")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED)),
            ok(
                    "select 1;",
                    "select 1;",
                    new String2StateConverter().appendKeyWord("select").append(" 1;").build()),
            ok(
                    "select 1; --comment",
                    "select 1;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1; ")
                            .appendLineComment("--comment")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED)),
            ok(
                    "select 1; /* comment */",
                    "select 1;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1; ")
                            .appendBlockComment("/* comment */")
                            .build()),
            ok(
                    "--comment \nselect 1; /* comment\n */",
                    "select 1;",
                    new String2StateConverter()
                            .appendLineComment("--comment \n")
                            .appendKeyWord("select")
                            .append(" 1; ")
                            .appendBlockComment("/* comment\n */")
                            .build()),
            ok(
                    "select '--'; /* comment\n */",
                    "select '--';",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("'--'")
                            .append("; ")
                            .appendBlockComment("/* comment\n */")
                            .build()),
            ok(
                    "--comment \nselect 1; /* comment\n */",
                    "select 1;",
                    new String2StateConverter()
                            .appendLineComment("--comment \n")
                            .appendKeyWord("select")
                            .append(" 1; ")
                            .appendBlockComment("/* comment\n */")
                            .build()),
            ok(
                    "select ';\n';",
                    "select ';\n';",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("';\n'")
                            .append(";")
                            .build()),
            ok(
                    "select 1;",
                    "select 1;",
                    new String2StateConverter().appendKeyWord("select").append(" 1;").build()),
            // hints should be preserved
            ok(
                    "select /*+ hint */ 1;",
                    "select /*+ hint */ 1;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendHint("/*+ hint */")
                            .append(" 1;")
                            .build()),
            ok(
                    "/*+ hint */",
                    "/*+ hint */",
                    new String2StateConverter().appendHint("/*+ hint */").build()),
            // missed semicolon
            nokSemicolon("quit", new String2StateConverter().append("quit").build()),
            nokSemicolon(
                    "select --comment",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendLineComment("--comment")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED)),
            nokSemicolon(
                    "select --comment 1;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendLineComment("--comment 1;")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED)),
            nokSemicolon(
                    "select ';\n'",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("';\n'")
                            .build()),
            // missed quote
            nokSingleQuote(
                    "select ';",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("';")
                            .build(SqlMultiLineParser.State.SINGLE_QUOTED)),
            nokSingleQuote(
                    "select '--\n;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("'--\n;")
                            .build(SqlMultiLineParser.State.SINGLE_QUOTED)),
            nokSingleQuote(
                    "select '--/*\n*/",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("'--/*\n*/")
                            .build(SqlMultiLineParser.State.SINGLE_QUOTED)),
            nokSingleQuote(
                    "select '([",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendQuoted("'([")
                            .build(SqlMultiLineParser.State.SINGLE_QUOTED)),
            // missed sql identifier quote
            nokSqlIdentifierQuote(
                    "select 1 as `a;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendSqlIdentifierQuoted("`a;")
                            .build(SqlMultiLineParser.State.SQL_IDENTIFIER_QUOTED)),
            nokSqlIdentifierQuote(
                    "select 1 as `'a;'",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendSqlIdentifierQuoted("`'a;'")
                            .build(SqlMultiLineParser.State.SQL_IDENTIFIER_QUOTED)),
            nokSqlIdentifierQuote(
                    "select 1 as `--a;'",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendSqlIdentifierQuoted("`--a;'")
                            .build(SqlMultiLineParser.State.SQL_IDENTIFIER_QUOTED)),
            nokSqlIdentifierQuote(
                    "select 1 as `/*a;'*/",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendSqlIdentifierQuoted("`/*a;'*/")
                            .build(SqlMultiLineParser.State.SQL_IDENTIFIER_QUOTED)),
            // missed closing of multiline comment or hint
            nokMissingCloseComment(
                    "select 1;/*+\n1 as `a;",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1;")
                            .appendHint("/*+\n1 as `a;")
                            .build(SqlMultiLineParser.State.HINT)),
            nokMissingCloseComment(
                    "select 1 as /*`'a;'",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendBlockComment("/*`'a;'")
                            .build(SqlMultiLineParser.State.MULTILINE_COMMENTED)),
            nokMissingCloseComment(
                    "select 1 as /*/",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendBlockComment("/*/")
                            .build(SqlMultiLineParser.State.MULTILINE_COMMENTED)),
            nokMissingCloseComment(
                    "select 1 as /*a;'* /",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" 1 ")
                            .appendKeyWord("as")
                            .append(" ")
                            .appendBlockComment("/*a;'* /")
                            .build(SqlMultiLineParser.State.MULTILINE_COMMENTED)),
            // missed brackets
            nokMissingOpenBracket(
                    "select [[1]]]",
                    new String2StateConverter().appendKeyWord("select").append(" [[1]]]").build(),
                    '['),
            nokMissingOpenBracket(
                    "select array[/**/array[row(1)]]]",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" array[")
                            .appendBlockComment("/**/")
                            .append("array[row(1)]]]")
                            .build(),
                    '['),
            nokMissingOpenBracket(
                    "select array['']]--",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" array[")
                            .appendQuoted("''")
                            .append("]]")
                            .appendLineComment("--")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED),
                    '['),
            nokMissingOpenBracket(
                    "select --\n((())())())--",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" ")
                            .appendLineComment("--\n")
                            .append("((())())())")
                            .appendLineComment("--")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED),
                    '('),
            nokMissingCloseBracket(
                    "select map[array[9]",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" map[array[9]")
                            .build(),
                    ']'),
            nokMissingCloseBracket(
                    "select (1 + 0/**/]'",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" (1 + 0")
                            .appendBlockComment("/**/")
                            .append("]")
                            .appendQuoted("'")
                            .build(SqlMultiLineParser.State.SINGLE_QUOTED),
                    ')'),
            nokMissingCloseBracket(
                    "select map[6, ''--]",
                    new String2StateConverter()
                            .appendKeyWord("select")
                            .append(" map[6, ")
                            .appendQuoted("''")
                            .appendLineComment("--]")
                            .build(SqlMultiLineParser.State.ONE_LINE_COMMENTED),
                    ']'),
        };
    }

    @Parameterized.Parameter() public String sql;

    @Parameterized.Parameter(1)
    public String expectedParsedWords;

    @Parameterized.Parameter(2)
    public SqlMultiLineParser.State[] expectedMask;

    @Parameterized.Parameter(3)
    public String expectedExceptionMessage;

    /*@Test
        public void testParsedCommentFreeLine() {
            if (expectedExceptionMessage == null) {
                Assert.assertEquals(
                        expectedParsedWords,
                        SqlMultiLineParser.getParsedCommentFreeLine(sql, () -> false, false, " "));
            } else {
                try {
                    PARSER.parse(sql, 0);
                    fail("Expected: " + expectedExceptionMessage);
                } catch (EOFError e) {
                    Assert.assertEquals(expectedExceptionMessage, e.getMessage());
                }
            }
        }
    */
    @Test
    public void testMask() {
        Assert.assertArrayEquals(
                expectedMask, PARSER.parse(sql, 0, Parser.ParseContext.COMPLETE, false).getState());
    }

    // ---------------------------------------------------------------------------------------------
    private static Object[] ok(String sql, String expected, SqlMultiLineParser.State[] mask) {
        return new Object[] {sql, expected, mask, null};
    }

    private static Object[] nokSemicolon(String sql, SqlMultiLineParser.State[] mask) {
        return new Object[] {sql, null, mask, "Missing semicolon"};
    }

    private static Object[] nokSingleQuote(String sql, SqlMultiLineParser.State[] mask) {
        return new Object[] {sql, null, mask, "Missing closing quote '"};
    }

    private static Object[] nokSqlIdentifierQuote(String sql, SqlMultiLineParser.State[] mask) {
        return new Object[] {sql, null, mask, "Missing sql identifier quote `"};
    }

    private static Object[] nokMissingOpenBracket(
            String sql, SqlMultiLineParser.State[] mask, char bracket) {
        return new Object[] {sql, null, mask, "Missing opening bracket " + bracket};
    }

    private static Object[] nokMissingCloseBracket(
            String sql, SqlMultiLineParser.State[] mask, char bracket) {
        return new Object[] {sql, null, mask, "Missing closing bracket " + bracket};
    }

    private static Object[] nokMissingCloseComment(String sql, SqlMultiLineParser.State[] mask) {
        return new Object[] {sql, null, mask, "Missing end of multiline comment */"};
    }

    private static class String2StateConverter {
        private final List<Pair<SqlMultiLineParser.State, String>> lines = new ArrayList<>();

        public String2StateConverter appendKeyWord(String line) {
            // TODO: should be State.KEYWORD after FLINK-24910 completed
            lines.add(Pair.of(SqlMultiLineParser.State.KEYWORD, line));
            return this;
        }

        public String2StateConverter appendLineComment(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.ONE_LINE_COMMENTED, line));
            return this;
        }

        public String2StateConverter appendBlockComment(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.MULTILINE_COMMENTED, line));
            return this;
        }

        public String2StateConverter appendQuoted(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.SINGLE_QUOTED, line));
            return this;
        }

        public String2StateConverter appendSqlIdentifierQuoted(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.SQL_IDENTIFIER_QUOTED, line));
            return this;
        }

        public String2StateConverter append(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.DEFAULT, line));
            return this;
        }

        public String2StateConverter appendHint(String line) {
            lines.add(Pair.of(SqlMultiLineParser.State.HINT, line));
            return this;
        }

        public String2StateConverter appendAs(String line, SqlMultiLineParser.State state) {
            lines.add(Pair.of(state, line));
            return this;
        }

        public SqlMultiLineParser.State[] build() {
            return build(SqlMultiLineParser.State.DEFAULT);
        }

        public SqlMultiLineParser.State[] build(SqlMultiLineParser.State endsWith) {
            int size = lines.stream().mapToInt(t -> t.getRight().length()).sum() + 1;
            SqlMultiLineParser.State[] states = new SqlMultiLineParser.State[size];
            int start = 0;
            for (Pair<SqlMultiLineParser.State, String> pair : lines) {
                for (int i = 0; i < pair.getRight().length(); i++) {
                    states[start + i] = pair.getLeft();
                }
                start += pair.getRight().length();
            }
            states[states.length - 1] = endsWith;
            return states;
        }
    }
}
