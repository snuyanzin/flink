package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowMaterializedTables extends SqlShowCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW MATERIALIZED TABLES", SqlKind.OTHER);

    public SqlShowMaterializedTables(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier databaseName,
            boolean notLike,
            SqlCharStringLiteral likeLiteral) {
        // only LIKE currently supported for SHOW MATERIALIZED TABLES
        super(
                pos,
                preposition,
                databaseName,
                likeLiteral == null ? null : "LIKE",
                likeLiteral,
                notLike);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    String getOperationName() {
        return getOperator().getName();
    }
}
