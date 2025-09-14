package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;

import org.apache.calcite.sql.SqlNode;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

public abstract class AbstractAlterMaterializedTableConverter<T extends SqlNode>
        implements SqlNodeConverter<T> {

    protected ObjectIdentifier resolveIdentifier(
            SqlAlterMaterializedTable sqlAlterMaterializedTable, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterMaterializedTable.fullTableName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected ResolvedCatalogMaterializedTable getResolvedMaterializedTable(
            ConvertContext context, ObjectIdentifier identifier, Supplier<String> errorMessage) {
        ResolvedCatalogBaseTable<?> table =
                context.getCatalogManager().getTableOrError(identifier).getResolvedTable();

        if (MATERIALIZED_TABLE != table.getTableKind()) {
            throw new ValidationException(errorMessage.get());
        }

        return (ResolvedCatalogMaterializedTable) table;
    }

    protected CatalogMaterializedTable buildUpdatedMaterializedTable(
            ResolvedCatalogMaterializedTable oldTable,
            Consumer<CatalogMaterializedTable.Builder> consumer) {

        CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(oldTable.getUnresolvedSchema())
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .options(oldTable.getOptions())
                        .definitionQuery(oldTable.getDefinitionQuery())
                        .distribution(oldTable.getDistribution().orElse(null))
                        .freshness(oldTable.getDefinitionFreshness())
                        .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                        .refreshMode(oldTable.getRefreshMode())
                        .refreshStatus(oldTable.getRefreshStatus())
                        .refreshHandlerDescription(
                                oldTable.getRefreshHandlerDescription().orElse(null))
                        .serializedRefreshHandler(oldTable.getSerializedRefreshHandler());

        consumer.accept(builder);
        return builder.build();
    }
}
