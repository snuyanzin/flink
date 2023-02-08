package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.Map;

/** {@link SqlOperator} for <code>ROW</code>. */
public class SqlRowConstructor extends SqlRowOperator {
    public SqlRowConstructor() {
        super("ROW");
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        return opBinding
                .getTypeFactory()
                .createStructType(
                        new AbstractList<Map.Entry<String, RelDataType>>() {
                            @Override
                            public Map.Entry<String, RelDataType> get(int index) {
                                return Pair.of(
                                        SqlUtil.deriveAliasFromOrdinal(index),
                                        opBinding.getOperandType(index));
                            }

                            @Override
                            public int size() {
                                return opBinding.getOperandCount();
                            }
                        });
    }
}
