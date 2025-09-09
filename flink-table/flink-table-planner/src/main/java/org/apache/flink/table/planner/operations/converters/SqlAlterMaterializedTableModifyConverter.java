package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableModify;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;

public class SqlAlterMaterializedTableModifyConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableModify> {

    @Override
    public Operation convertSqlNode(SqlAlterMaterializedTableModify node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        return new AlterMaterializedTableSuspendOperation(identifier);
    }
}
