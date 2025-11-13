package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import java.util.stream.Collectors;

public abstract class SqlAlterMaterializedTableSchemaConverter<
                T extends SqlAlterMaterializedTableSchema>
        extends AbstractAlterMaterializedTableConverter<T> {

    @Override
    protected Operation convertToOperation(
            T alterMaterializedTableSchema,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context) {
        SchemaConverter<ResolvedCatalogMaterializedTable> converter =
                createSchemaConverter(alterMaterializedTableSchema, oldMaterializedTable, context);
        converter.updateColumn(alterMaterializedTableSchema.getColumnPositions().getList());
        alterMaterializedTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterMaterializedTableSchema.getDistribution().ifPresent(converter::updateDistribution);
        alterMaterializedTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        Schema schema = converter.convert();
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldMaterializedTable, builder -> builder.schema(schema));

        final ObjectIdentifier identifier = getIdentifier(alterMaterializedTableSchema, context);
        return new AlterMaterializedTableChangeOperation(
                identifier,
                converter.getChangesCollector().stream()
                        .map(t -> (TableChange.MaterializedTableChange) t)
                        .collect(Collectors.toList()),
                updatedTable);
    }

    protected abstract SchemaConverter<ResolvedCatalogMaterializedTable> createSchemaConverter(
            SqlAlterMaterializedTableSchema alterMaterializedTableSchema,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context);
}
