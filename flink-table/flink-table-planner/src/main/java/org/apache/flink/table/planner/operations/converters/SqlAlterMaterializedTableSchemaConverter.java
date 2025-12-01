package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableAddSchema;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableModifySchema;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.table.MergeTableAsUtil;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

public abstract class SqlAlterMaterializedTableSchemaConverter<
                T extends SqlAlterMaterializedTableSchema>
        extends AbstractAlterMaterializedTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
        final SqlNode originalQuery =
                context.getFlinkPlanner().parser().parse(oldTable.getOriginalQuery());
        final SqlNode validateQuery = context.getSqlValidator().validate(originalQuery);
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));
        MaterializedTableUtils.validatePhysicalColumnsUsedByQuery(
                alterTableSchema.getColumnPositions(), queryOperation.getResolvedSchema());

        SchemaConverter converter = createSchemaConverter(oldTable, context);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);
        Schema schema = converter.convert();
        CatalogMaterializedTable mtWithUpdatedSchema =
                buildUpdatedMaterializedTable(oldTable, builder -> builder.schema(schema));

        // If needed, rewrite the query to include the new fields in the select list
        PlannerQueryOperation updatedQueryOperation =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                context.getCatalogManager(),
                                context.getFlinkPlanner(),
                                validateQuery,
                                context.getCatalogManager()
                                        .resolveCatalogMaterializedTable(mtWithUpdatedSchema),
                                extractComputedColumn(alterTableSchema),
                                extractMetadataColumn(alterTableSchema));

        CatalogMaterializedTable mtWithUpdatedSchemaAndQuery =
                buildUpdatedMaterializedTable(
                        oldTable,
                        builder -> {
                            builder.schema(schema);
                            builder.expandedQuery(updatedQueryOperation.asSerializableString());
                        });

        return new AlterMaterializedTableChangeOperation(
                resolveIdentifier(alterTableSchema, context),
                converter.changesCollector,
                mtWithUpdatedSchemaAndQuery);
    }

    private List<SqlTableColumn.SqlComputedColumn> extractComputedColumn(T alterTableSchema) {
        List<SqlTableColumn.SqlComputedColumn> computedColumns = new ArrayList<>();
        for (SqlNode node : alterTableSchema.getColumnPositions()) {
            if (node instanceof SqlTableColumnPosition) {
                if (((SqlTableColumnPosition) node).getColumn()
                        instanceof SqlTableColumn.SqlComputedColumn) {
                    computedColumns.add(
                            (SqlTableColumn.SqlComputedColumn)
                                    ((SqlTableColumnPosition) node).getColumn());
                }
            }
        }
        return computedColumns;
    }

    private List<SqlTableColumn.SqlMetadataColumn> extractMetadataColumn(T alterTableSchema) {
        List<SqlTableColumn.SqlMetadataColumn> metadataColumns = new ArrayList<>();
        for (SqlNode node : alterTableSchema.getColumnPositions()) {
            if (node instanceof SqlTableColumnPosition) {
                if (((SqlTableColumnPosition) node).getColumn()
                        instanceof SqlTableColumn.SqlMetadataColumn) {
                    metadataColumns.add(
                            (SqlTableColumn.SqlMetadataColumn)
                                    ((SqlTableColumnPosition) node).getColumn());
                }
            }
        }
        return metadataColumns;
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
