package org.apache.flink.table.planner.operations;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogOptionsOperation;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SqlShowToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    void testConvert() {
        // test alter catalog options
        final String sql1 = "SHOW TABLES";

        Operation operation = parse(sql1);
        assertThat(operation)
                .isInstanceOf(ShowTablesOperation.class);
        assertThat(operation.asSummaryString()).isEqualTo("SHOW TABLES");

    }
}
