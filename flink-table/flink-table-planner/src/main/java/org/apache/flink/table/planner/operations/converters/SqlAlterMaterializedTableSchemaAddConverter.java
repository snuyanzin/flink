package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableAdd;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;

public class SqlAlterMaterializedTableSchemaAddConverter
        extends SqlAlterMaterializedTableSchemaConverter<SqlAlterMaterializedTableAdd> {
    @Override
    protected SchemaConverter<ResolvedCatalogMaterializedTable> createSchemaConverter(
            SqlAlterMaterializedTableSchema alterMaterializedTableSchema,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context) {
        return new SchemaAddConverter<>(
                oldMaterializedTable, context, oldMaterializedTable.getDistribution().orElse(null));
    }
}
