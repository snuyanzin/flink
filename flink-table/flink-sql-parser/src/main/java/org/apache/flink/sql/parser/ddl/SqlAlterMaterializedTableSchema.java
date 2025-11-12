package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

public class SqlAlterMaterializedTableSchema extends SqlAlterMaterializedTable
        implements ExtendedSqlNode {
    protected final SqlNodeList columnList;
    @Nullable protected final SqlWatermark watermark;
    @Nullable protected final SqlDistribution distribution;
    protected final List<SqlTableConstraint> constraints;

    public SqlAlterMaterializedTableSchema(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark sqlWatermark,
            @Nullable SqlDistribution distribution) {
        super(pos, tableName);
        this.columnList = columnList;
        this.constraints = constraints;
        this.distribution = distribution;
        this.watermark = sqlWatermark;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }

    @Override
    public void validate() throws SqlValidateException {}

    void unparseSchemaAndDistribution(SqlWriter writer, int leftPrec, int rightPrec) {
        if ((columnList != null && !columnList.isEmpty())
                || (constraints != null && !constraints.isEmpty())
                || watermark != null) {
            SqlUnparseUtils.unparseTableSchema(
                    writer, leftPrec, rightPrec, columnList, constraints, watermark);
        }
        if (distribution != null) {
            distribution.unparseAlter(writer, leftPrec, rightPrec);
        }
    }
}
