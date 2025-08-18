package org.apache.flink.table.operations;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;

public class ShowMaterializedTablesOperation extends AbstractShowOperation {
    private final @Nullable String databaseName;
    public ShowMaterializedTablesOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable String preposition,
            @Nullable ShowLikeOperator likeOp) {
        super(catalogName, preposition, likeOp);
        this.databaseName = databaseName;
    }

    public ShowMaterializedTablesOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            @Nullable ShowLikeOperator likeOp) {
        this(catalogName, databaseName, null, likeOp);
    }

    @Override
    protected Collection<String> retrieveDataForTableResult(Context ctx) {
        final CatalogManager catalogManager = ctx.getCatalogManager();
        final String qualifiedCatalogName = catalogManager.qualifyCatalog(catalogName);
        final String qualifiedDatabaseName = catalogManager.qualifyDatabase(databaseName);
        if (preposition == null) {
            return catalogManager.listTables();
        } else {
            Catalog catalog = catalogManager.getCatalogOrThrowException(qualifiedCatalogName);
            if (catalog.databaseExists(qualifiedDatabaseName)) {
                return catalogManager.listTables(qualifiedCatalogName, qualifiedDatabaseName);
            } else {
                throw new ValidationException(
                        String.format(
                                "Database '%s'.'%s' doesn't exist.",
                                qualifiedCatalogName, qualifiedDatabaseName));
            }
        }
    }

    @Override
    protected String getOperationName() {
        return "";
    }

    @Override
    protected String getColumnName() {
        return "";
    }
}
