package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropWatermark;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import java.util.List;
import java.util.function.Function;

public class SqlAlterMaterializedTableDropWatermarkConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableDropWatermark> {
    @Override
    protected Operation convertToOperation(
            SqlAlterMaterializedTableDropWatermark dropWatermark,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context) {
        if (oldMaterializedTable.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base materialized table does not define any watermark strategy.",
                            EX_MSG_PREFIX));
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        SchemaReferencesManager.buildUpdatedColumn(
                schemaBuilder,
                oldMaterializedTable,
                (builder, column) -> builder.fromColumns(List.of(column)));
        SchemaReferencesManager.buildUpdatedPrimaryKey(
                schemaBuilder, oldMaterializedTable, Function.identity());

        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldMaterializedTable,
                        builder -> builder.schema(schemaBuilder.build()));

        ObjectIdentifier identifier = resolveIdentifier(dropWatermark, context);
        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.dropWatermark()), updatedTable);
    }
}
