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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.fail;

/** Test {@link SqlMultiLineParser}. */
@RunWith(Parameterized.class)
public class SqlMultilineParserTest {

    @Parameterized.Parameters(
            name = "Parsing: {0}. Expected identifier: {1}, Expected exception message: {2}")
    public static Object[][] parameters() {
        return new Object[][] {
            // valid
            ok("quit;", "quit;"),
            ok("--select 1;", ""),
            ok("select 1;", "select 1;"),
            ok("select 1; --comment", "select 1;"),
            ok("select 1; /* comment */", "select 1;"),
            ok("--comment \nselect 1; /* comment\n */", "select 1;"),
            ok("select '--'; /* comment\n */", "select '--';"),
            ok("--comment \nselect 1; /* comment\n */", "select 1;"),
            ok("select ';\n';", "select ';\n';"),
            ok("select 1;", "select 1;"),
            // hints should be preserved
            ok("select /*+ hint */ 1;", "select /*+ hint */ 1;"),
            // missed semicolon
            nokSemicolon("quit"),
            nokSemicolon("select --comment"),
            nokSemicolon("select --comment 1;"),
            nokSemicolon("select ';\n'"),
            // missed quote
            nokSingleQuote("select ';"),
            nokSingleQuote("select '--\n;"),
            nokSingleQuote("select '--/*\n*/"),
            nokSingleQuote("select '(["),
            // missed sql identifier quote
            nokSqlIdentifierQuote("select 1 as `a;"),
            nokSqlIdentifierQuote("select 1 as `'a;'"),
            nokSqlIdentifierQuote("select 1 as `--a;'"),
            nokSqlIdentifierQuote("select 1 as `/*a;'*/"),
            // missed closing of multiline comment or hint
            nokMissingCloseComment("select 1;/*+\n1 as `a;"),
            nokMissingCloseComment("select 1 as /*`'a;'"),
            nokMissingCloseComment("select 1 as /*/"),
            nokMissingCloseComment("select 1 as /*a;'* /"),
            // missed brackets
            nokMissingOpenBracket("select [[1]]]", '['),
            nokMissingOpenBracket("select array[/**/array[row(1)]]]'", '['),
            nokMissingOpenBracket("select array['']]--", '['),
            nokMissingOpenBracket("select --\n((())())())--", '('),
            nokMissingCloseBracket("select map[array[9]", ']'),
            nokMissingCloseBracket("select (1 + 0/**/]'", ')'),
            nokMissingCloseBracket("select map[6, ''--]", ']'),
        };
    }

    @Parameterized.Parameter() public String sql;

    @Parameterized.Parameter(1)
    public String expectedParsedWords;

    @Parameterized.Parameter(2)
    public String expectedExceptionMessage;

    @Test
    public void myParseTest() {
        if (expectedExceptionMessage == null) {
            Assert.assertEquals(
                    expectedParsedWords, SqlMultiLineParser.getParsedCommentFreeLine(sql, false));
        } else {
            try {
                new SqlMultiLineParser().parse(sql, 0);
                fail("Expected: " + expectedExceptionMessage);
            } catch (EOFError e) {
                Assert.assertEquals(expectedExceptionMessage, e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    private static Object[] ok(String sql, String expected) {
        return new Object[] {sql, expected, null};
    }

    private static Object[] nokSemicolon(String sql) {
        return new Object[] {sql, null, "Missing semicolon"};
    }

    private static Object[] nokSingleQuote(String sql) {
        return new Object[] {sql, null, "Missing closing quote '"};
    }

    private static Object[] nokSqlIdentifierQuote(String sql) {
        return new Object[] {sql, null, "Missing sql identifier quote `"};
    }

    private static Object[] nokMissingOpenBracket(String sql, char bracket) {
        return new Object[] {sql, null, "Missing opening bracket " + bracket};
    }

    private static Object[] nokMissingCloseBracket(String sql, char bracket) {
        return new Object[] {sql, null, "Missing closing bracket " + bracket};
    }

    private static Object[] nokMissingCloseComment(String sql) {
        return new Object[] {sql, null, "Missing end of multiline comment */"};
    }
}
