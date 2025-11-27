package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableDropColumn;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.table.MergeTableAsUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlAlterMaterializedTableDropColumnConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableDropColumn> {
    @Override
    protected Operation convertToOperation(
            SqlAlterMaterializedTableDropColumn sqlAlterMaterializedTableDropColumn,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context) {

        final SqlNode originalQuery =
                context.getFlinkPlanner().parser().parse(oldMaterializedTable.getOriginalQuery());
        final SqlNode validateQuery = context.getSqlValidator().validate(originalQuery);
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));

        Set<String> columnsToDrop = new HashSet<>();
        sqlAlterMaterializedTableDropColumn
                .getColumnList()
                .forEach(
                        identifier -> {
                            String name = getColumnName((SqlIdentifier) identifier);
                            if (!columnsToDrop.add(name)) {
                                throw new ValidationException(
                                        String.format(
                                                "%sDuplicate column `%s`.", EX_MSG_PREFIX, name));
                            }
                        });

        SchemaReferencesManager referencesManager =
                SchemaReferencesManager.create(oldMaterializedTable);
        // Sort by dependencies count from smallest to largest. For example, when dropping column a,
        // b(b as a+1), the order should be: [b, a] after sort.
        List<String> sortedColumnsToDrop =
                columnsToDrop.stream()
                        .sorted(
                                Comparator.comparingInt(
                                                col ->
                                                        referencesManager.getColumnDependencyCount(
                                                                (String) col))
                                        .reversed())
                        .collect(Collectors.toList());
        List<TableChange> tableChanges = new ArrayList<>(sortedColumnsToDrop.size());
        for (String columnToDrop : sortedColumnsToDrop) {
            referencesManager.dropColumn(columnToDrop, () -> EX_MSG_PREFIX);
            tableChanges.add(TableChange.dropColumn(columnToDrop));
        }

        final Schema schema = getUpdatedSchema(oldMaterializedTable, columnsToDrop);

        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(
                        oldMaterializedTable, builder -> builder.schema(schema));

        PlannerQueryOperation queryOperation2 =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                context.getCatalogManager(),
                                context.getFlinkPlanner(),
                                queryOperation,
                                validateQuery,
                                context.getCatalogManager()
                                        .resolveCatalogMaterializedTable(updatedTable));

        ObjectIdentifier identifier =
                resolveIdentifier(sqlAlterMaterializedTableDropColumn, context);
        return new AlterMaterializedTableChangeOperation(
                identifier, List.of(TableChange.dropColumn("a")), updatedTable);
    }

    protected static String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAlter nested row type %s is not supported yet.",
                            EX_MSG_PREFIX, identifier));
        }
        return identifier.getSimple();
    }

    private Schema getUpdatedSchema(
            ResolvedCatalogBaseTable<?> oldTable, Set<String> columnsToDrop) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        SchemaReferencesManager.buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (!columnsToDrop.contains(column.getName())) {
                        builder.fromColumns(Collections.singletonList(column));
                    }
                });
        SchemaReferencesManager.buildUpdatedPrimaryKey(
                schemaBuilder, oldTable, Function.identity());
        SchemaReferencesManager.buildUpdatedWatermark(schemaBuilder, oldTable);
        return schemaBuilder.build();
    }
}
