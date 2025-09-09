package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import java.util.List;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

public class SqlAlterMaterializedTableDropDistributionConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableDropDistribution> {
    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableDropDistribution node, ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(node, context);

        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(context, identifier);

        if (oldTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not have a distribution to drop.", identifier));
        }
        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable = buildUpdatedMaterializedTable(oldTable);

        return new AlterMaterializedTableChangeOperation(identifier, List.of(TableChange.dropDistribution()), updatedTable);
    }

    private ObjectIdentifier resolveIdentifier(
            SqlAlterMaterializedTableDropDistribution sqlAlterTableAsQuery,
            ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTableAsQuery.fullTableName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    private ResolvedCatalogMaterializedTable getResolvedMaterializedTable(
            ConvertContext context, ObjectIdentifier identifier) {
        ResolvedCatalogBaseTable<?> baseTable =
                context.getCatalogManager().getTableOrError(identifier).getResolvedTable();
        if (MATERIALIZED_TABLE != baseTable.getTableKind()) {
            throw new ValidationException(
                    "Only materialized table support modify definition query.");
        }
        return (ResolvedCatalogMaterializedTable) baseTable;
    }

    private CatalogMaterializedTable buildUpdatedMaterializedTable(
            ResolvedCatalogMaterializedTable oldTable) {

        return CatalogMaterializedTable.newBuilder()
                .schema(oldTable.getUnresolvedSchema())
                .comment(oldTable.getComment())
                .partitionKeys(oldTable.getPartitionKeys())
                .options(oldTable.getOptions())
                .definitionQuery(oldTable.getDefinitionQuery())
                .freshness(oldTable.getDefinitionFreshness())
                .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                .refreshMode(oldTable.getRefreshMode())
                .refreshStatus(oldTable.getRefreshStatus())
                .refreshHandlerDescription(oldTable.getRefreshHandlerDescription().orElse(null))
                .serializedRefreshHandler(oldTable.getSerializedRefreshHandler())
                .build();
    }
}
