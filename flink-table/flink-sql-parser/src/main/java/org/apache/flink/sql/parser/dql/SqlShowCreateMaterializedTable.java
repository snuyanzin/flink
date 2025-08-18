package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlShowCreateMaterializedTable extends SqlShowCreate {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW CREATE MATERIALIZED TABLE", SqlKind.OTHER_DDL);

    public SqlShowCreateMaterializedTable(SqlParserPos pos, SqlIdentifier sqlIdentifier) {
        super(pos, sqlIdentifier);
    }

    public SqlIdentifier getMaterializedTableName() {
        return sqlIdentifier;
    }

    public String[] getFullMaterializedTableName() {
        return sqlIdentifier.names.toArray(new String[0]);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(sqlIdentifier);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(OPERATOR.getName());
        sqlIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
