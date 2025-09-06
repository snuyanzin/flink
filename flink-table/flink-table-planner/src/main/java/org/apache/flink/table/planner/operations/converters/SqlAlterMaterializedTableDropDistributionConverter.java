package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropDistribution;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;

public class SqlAlterMaterializedTableDropDistributionConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableDropDistribution> {
    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableDropDistribution node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        return new AlterMaterializedTableSuspendOperation(identifier);
    }
}
