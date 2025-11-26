package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableAddSchema;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableModifySchema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

public abstract class SqlAlterMaterializedTableSchemaConverter<
                T extends SqlAlterMaterializedTableSchema>
        extends AbstractAlterMaterializedTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
        SchemaConverter converter = createSchemaConverter(oldTable, context);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldTable, builder -> builder.schema(converter.convert()));
        return new AlterMaterializedTableChangeOperation(
                resolveIdentifier(alterTableSchema, context),
                converter.changesCollector,
                updatedTable);
    }

    protected abstract SchemaConverter createSchemaConverter(
            ResolvedCatalogMaterializedTable oldMaterializedTable, ConvertContext context);

    public static class SqlAlterMaterializedTableAddSchemaConverter
            extends SqlAlterMaterializedTableSchemaConverter<SqlAlterMaterializedTableAddSchema> {
        @Override
        protected SchemaConverter createSchemaConverter(
                ResolvedCatalogMaterializedTable oldMaterializedTable, ConvertContext context) {
            return new SchemaAddConverter(oldMaterializedTable, context);
        }
    }

    public static class SqlAlterMaterializedTableModifySchemaConverter
            extends SqlAlterMaterializedTableSchemaConverter<
                    SqlAlterMaterializedTableModifySchema> {
        @Override
        protected SchemaConverter createSchemaConverter(
                ResolvedCatalogMaterializedTable oldMaterializedTable, ConvertContext context) {
            return new SchemaModifyConverter(oldMaterializedTable, context);
        }
    }
}
