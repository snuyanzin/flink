package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import java.util.List;

public class SqlAlterMaterializedTableDropDistributionConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableDropDistribution> {
    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableDropDistribution node, ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(node, context);

        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(
                        context,
                        identifier,
                        () -> "Operation is supported only for materialized tables");

        if (oldTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not have a distribution to drop.",
                            identifier));
        }

        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(oldTable, builder -> builder.distribution(null));

        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.dropDistribution()), updatedTable);
    }
}
