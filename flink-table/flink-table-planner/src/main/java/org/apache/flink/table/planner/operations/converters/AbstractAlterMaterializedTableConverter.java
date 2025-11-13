/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTable;
import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableSchema;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange.MaterializedTableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.utils.ValidationUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

/** Abstract converter for {@link org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTable}. */
public abstract class AbstractAlterMaterializedTableConverter<T extends SqlAlterMaterializedTable>
        implements SqlNodeConverter<T> {

    protected abstract Operation convertToOperation(
            T sqlAlterTable,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            ConvertContext context);

    protected final Schema getOldSchema(
            ResolvedCatalogMaterializedTable resolvedCatalogMaterializedTable) {
        return resolvedCatalogMaterializedTable.getUnresolvedSchema();
    }

    protected final TableDistribution getOldTableDistribution(
            ResolvedCatalogMaterializedTable resolvedCatalogTable) {
        return resolvedCatalogTable.getDistribution().orElse(null);
    }

    protected final List<String> getOldPartitionKeys(
            ResolvedCatalogMaterializedTable resolvedCatalogMaterializedTable) {
        return resolvedCatalogMaterializedTable.getPartitionKeys();
    }

    protected final String getOldComment(
            ResolvedCatalogMaterializedTable resolvedCatalogMaterializedTable) {
        return resolvedCatalogMaterializedTable.getComment();
    }

    protected final Map<String, String> getOldOptions(ResolvedCatalogTable resolvedCatalogTable) {
        return resolvedCatalogTable.getOptions();
    }

    @Override
    public final Operation convertSqlNode(T sqlAlterMaterializedTable, ConvertContext context) {
        CatalogManager catalogManager = context.getCatalogManager();
        final ObjectIdentifier tableIdentifier = getIdentifier(sqlAlterMaterializedTable, context);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);

        if (optionalCatalogTable.isEmpty() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        ValidationUtils.validateTableKind(
                optionalCatalogTable.get().getTable(),
                MATERIALIZED_TABLE,
                "alter materialized table");

        return convertToOperation(
                sqlAlterMaterializedTable, optionalCatalogTable.get().getResolvedTable(), context);
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
                        .originalQuery(oldTable.getOriginalQuery())
                        .expandedQuery(oldTable.getExpandedQuery())
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

    protected final Operation buildAlterTableChangeOperation(
            SqlAlterMaterializedTable alterMaterializedTable,
            List<MaterializedTableChange> tableChanges,
            Schema newSchema,
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            CatalogManager catalogManager) {
        final TableDistribution tableDistribution =
                getTableDistribution(alterMaterializedTable, oldMaterializedTable);

        CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(oldMaterializedTable.getUnresolvedSchema())
                        .comment(oldMaterializedTable.getComment())
                        .partitionKeys(oldMaterializedTable.getPartitionKeys())
                        .options(oldMaterializedTable.getOptions())
                        .definitionQuery(oldMaterializedTable.getDefinitionQuery())
                        .distribution(tableDistribution)
                        .freshness(oldMaterializedTable.getDefinitionFreshness())
                        .logicalRefreshMode(oldMaterializedTable.getLogicalRefreshMode())
                        .refreshMode(oldMaterializedTable.getRefreshMode())
                        .refreshStatus(oldMaterializedTable.getRefreshStatus())
                        .refreshHandlerDescription(
                                oldMaterializedTable.getRefreshHandlerDescription().orElse(null))
                        .serializedRefreshHandler(
                                oldMaterializedTable.getSerializedRefreshHandler());

        return new AlterMaterializedTableChangeOperation(
                catalogManager.qualifyIdentifier(
                        UnresolvedIdentifier.of(alterMaterializedTable.fullTableName())),
                tableChanges,
                builder.build());
    }

    protected final ObjectIdentifier getIdentifier(
            SqlAlterMaterializedTable node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected TableDistribution getTableDistribution(
            SqlAlterMaterializedTable alterMaterializedTable,
            ResolvedCatalogMaterializedTable oldMaterializedTable) {
        if (alterMaterializedTable instanceof SqlAlterMaterializedTableSchema) {
            final Optional<TableDistribution> tableDistribution =
                    ((SqlAlterMaterializedTableSchema) alterMaterializedTable)
                            .getDistribution()
                            .map(OperationConverterUtils::getDistributionFromSqlDistribution);
            if (tableDistribution.isPresent()) {
                return tableDistribution.get();
            }
        }
        return oldMaterializedTable.getDistribution().orElse(null);
    }
}
