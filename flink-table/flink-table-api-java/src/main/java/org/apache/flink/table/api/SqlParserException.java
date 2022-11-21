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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception for all errors occurring during sql parsing.
 *
 * <p>This exception indicates that the SQL parse failed.
 */
@PublicEvolving
public class SqlParserException extends RuntimeException {

    private final int lineNum;
    private final int lineNumEnd;
    private final int columnNum;
    private final int columnNumEnd;

    public SqlParserException(
            String message,
            Throwable cause,
            int columnNum,
            int lineNum,
            int columnNumEnd,
            int lineNumEnd) {
        super(message, cause);
        this.columnNum = columnNum;
        this.lineNum = lineNum;
        this.columnNumEnd = columnNumEnd;
        this.lineNumEnd = lineNumEnd;
    }

    public SqlParserException(String message, Throwable cause) {
        this(message, cause, -1, -1, -1, -1);
    }

    public SqlParserException(String message) {
        super(message);
        this.columnNum = -1;
        this.lineNum = -1;
        this.columnNumEnd = -1;
        this.lineNumEnd = -1;
    }

    public int getLineNum() {
        return lineNum;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public int getLineEnd() {
        return lineNumEnd;
    }

    public int getColumnNumEnd() {
        return columnNumEnd;
    }
}
