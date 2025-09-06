package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableAdd;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;

public class SqlAlterMaterializedTableAddConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableAdd> {
    @Override
    public Operation convertSqlNode(SqlAlterMaterializedTableAdd node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        return new AlterMaterializedTableSuspendOperation(identifier);
    }
}
