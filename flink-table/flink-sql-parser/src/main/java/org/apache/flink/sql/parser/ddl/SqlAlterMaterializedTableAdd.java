package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

public class SqlAlterMaterializedTableAdd extends SqlAlterMaterializedTable {
    protected final @Nullable SqlDistribution distribution;

    public SqlAlterMaterializedTableAdd(
            SqlParserPos pos, SqlIdentifier tableName, SqlDistribution distribution) {
        super(pos, tableName);
        this.distribution = distribution;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(getTableName(), distribution);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword(" ADD ");
        if (distribution != null) {
            distribution.unparseAlter(writer, leftPrec, rightPrec);
        }
    }
}
