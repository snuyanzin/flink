package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW CREATE MATERIALIZED TABLE statement. */
@Internal
public class ShowCreateMaterializedTableOperation implements ShowOperation {
    private final ObjectIdentifier tableIdentifier;

    public ShowCreateMaterializedTableOperation(ObjectIdentifier sqlIdentifier) {
        this.tableIdentifier = sqlIdentifier;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "SHOW CREATE MATERIALIZED TABLE %s", tableIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ContextResolvedTable table =
                ctx.getCatalogManager()
                        .getTable(tableIdentifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Could not execute SHOW CREATE MATERIALIZED TABLE. Materialized table with identifier %s does not exist.",
                                                        tableIdentifier.asSerializableString())));
        String resultRow =
                ShowCreateUtil.buildShowCreateMaterializedTableRow(
                        table.getResolvedTable(),
                        tableIdentifier,
                        table.isTemporary(),
                        ctx.getCatalogManager().getSqlFactory());

        return buildStringArrayResult("result", new String[] {resultRow});
    }
}
