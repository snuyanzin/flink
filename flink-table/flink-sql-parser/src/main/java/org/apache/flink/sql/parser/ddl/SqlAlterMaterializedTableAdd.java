package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

public class SqlAlterMaterializedTableAdd extends SqlAlterMaterializedTableSchema {

    public SqlAlterMaterializedTableAdd(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark sqlWatermark,
            @Nullable SqlDistribution distribution) {
        super(pos, tableName, columnList, constraints, sqlWatermark, distribution);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        // unparse table schema and distribution
        unparseSchemaAndDistribution(writer, leftPrec, rightPrec);
    }
}
