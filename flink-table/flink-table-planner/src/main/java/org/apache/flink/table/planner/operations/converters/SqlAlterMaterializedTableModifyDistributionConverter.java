package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableModifyDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.List;

public class SqlAlterMaterializedTableModifyDistributionConverter
        extends AbstractAlterMaterializedTableConverter<
                SqlAlterMaterializedTableModifyDistribution> {
    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableModifyDistribution node, ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(node, context);

        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(
                        context,
                        identifier,
                        () -> "Operation is supported only for materialized tables");

        if (oldTable.getDistribution().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not have a distribution to modify.",
                            identifier));
        }

        TableDistribution tableDistribution =
                OperationConverterUtils.getDistributionFromSqlDistribution(
                        node.getDistribution().get());
        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldTable, builder -> builder.distribution(tableDistribution));

        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.modify(tableDistribution)), updatedTable);
    }
}
