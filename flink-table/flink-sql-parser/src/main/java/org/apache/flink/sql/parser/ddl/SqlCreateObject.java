/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/** Base class for CREATE DDL sql calls. */
public abstract class SqlCreateObject extends SqlCreate {
    private final SqlIdentifier name;
    private final boolean isTemporary;
    private final @Nullable SqlCharStringLiteral comment;
    private final @Nullable SqlNodeList properties;

    public SqlCreateObject(
            SqlOperator operator,
            SqlParserPos pos,
            SqlIdentifier name,
            boolean isTemporary,
            boolean replace,
            boolean ifNotExists,
            @Nullable SqlNodeList properties,
            @Nullable SqlCharStringLiteral comment) {
        super(operator, pos, replace, ifNotExists);
        this.name = requireNonNull(name, "name should not be null");
        this.comment = comment;
        this.isTemporary = isTemporary;
        this.properties = properties;
    }

    /** The scope will be used in unparse. */
    protected abstract String getScope();

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public @Nullable SqlCharStringLiteral getComment() {
        return comment;
    }

    public SqlNodeList getProperties() {
        return properties;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public String[] getFullName() {
        return name.names.toArray(new String[0]);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparseCreateIfNotExists(writer, leftPrec, rightPrec);
        UnparseUtils.unparseComment(comment, writer, leftPrec, rightPrec);
        unparseProperties(writer, leftPrec, rightPrec);
    }

    protected void unparseCreateIfNotExists(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword(getScope());
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }

    protected void unparseProperties(SqlWriter writer, int leftPrec, int rightPrec) {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("WITH");
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode property : properties) {
            SqlUnparseUtils.printIndent(writer);
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }
}
