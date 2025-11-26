package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlAlterMaterializedTableDropWatermark extends SqlAlterMaterializedTable {

    public SqlAlterMaterializedTableDropWatermark(SqlParserPos pos, SqlIdentifier tableName) {
        super(pos, tableName);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword("DROP WATERMARK");
    }
}
