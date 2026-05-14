package org.apache.flink.table.planner.plan.schema;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

public class TimeIndicatorRelDataType extends BasicSqlType {
    private final boolean isEventTime;
    private final BasicSqlType originalType;

    public TimeIndicatorRelDataType(
            RelDataTypeSystem typeSystem,
            BasicSqlType originalType,
            boolean nullable,
            boolean isEventTime) {
        super(typeSystem, originalType.getSqlTypeName(), nullable, originalType.getPrecision());
        this.isEventTime = isEventTime;
        this.originalType = originalType;
        computeDigest();
    }

    public boolean isEventTime() {
        return isEventTime;
    }

    public BasicSqlType getOriginalType() {
        return originalType;
    }

    public RelDataTypeSystem getTypeSystem() {
        return typeSystem;
    }

    @Override
    public int hashCode() {
        return super.hashCode()
                + 42; // we change the hash code to differentiate from regular timestamps
    }

    @Override
    public String toString() {
        // Calcite caches type instance by the type string representation in
        // org.apache.calcite.rel.type.RelDataTypeFactoryImpl, thus we use
        // unique name for each TimeIndicatorRelDataType
        final String typeNameStr =
                (typeName == SqlTypeName.TIMESTAMP) ? "TIMESTAMP(3)" : "TIMESTAMP_LTZ(3)";
        return typeNameStr + (isEventTime ? "*ROWTIME*" : "*PROCTIME*");
    }
}
