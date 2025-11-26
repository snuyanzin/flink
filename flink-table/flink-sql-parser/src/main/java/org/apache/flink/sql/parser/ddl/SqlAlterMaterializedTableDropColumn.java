package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SqlAlterMaterializedTableDropColumn extends SqlAlterMaterializedTable {
    private final SqlNodeList columnList;

    public SqlAlterMaterializedTableDropColumn(
            SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnList) {
        super(pos, tableName);
        this.columnList = columnList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, columnList);
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword("DROP");
        // unparse table column
        SqlUnparseUtils.unparseTableSchema(
                columnList, Collections.emptyList(), null, writer, leftPrec, rightPrec);
    }
}
