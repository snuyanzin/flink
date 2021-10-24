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

import java.util.ArrayList;
import java.util.List;

/**
 * Line splitter to determine whether the submitted line is complete. It also offers to split the
 * submitted content into multiple statements. It removes one line and multiline (NOT hints)
 * comments.
 */
public class CliStatementSplitter {

    public static List<String> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        for (String line : content.split("\n")) {
            sb.append(line).append("\n");
            String stmt =
                    SqlMultiLineParser.getParsedCommentFreeLine(sb.toString(), () -> false).trim();
            if (stmt.endsWith(";")) {
                statements.add(stmt);
                sb.setLength(0);
            }
        }
        if (sb.length() > 0) {
            String stmt =
                    SqlMultiLineParser.getParsedCommentFreeLine(sb.toString(), () -> false).trim();
            if (!stmt.isEmpty()) {
                statements.add(stmt);
            }
        }
        return statements;
    }
}
