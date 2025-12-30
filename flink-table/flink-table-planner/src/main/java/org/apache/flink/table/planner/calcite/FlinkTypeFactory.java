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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.ExtendedRelTypeFactory;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.planner.plan.schema.GenericRelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DescriptorType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Flink specific type factory that represents the interface between Flink's [[LogicalType]] and
 * Calcite's [[RelDataType]].
 */
public class FlinkTypeFactory extends JavaTypeFactoryImpl implements ExtendedRelTypeFactory {
    private final ClassLoader classLoader;
    private final Map<LogicalType, RelDataType> seenTypes = new HashMap<>();

    public FlinkTypeFactory(ClassLoader classLoader, RelDataTypeSystem typeSystem) {
        super(typeSystem);
        this.classLoader = classLoader;
    }

    public FlinkTypeFactory(ClassLoader classLoader) {
        this(classLoader, FlinkTypeSystem.INSTANCE);
    }

    /**
     * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
     *
     * @param tableSchema schema to convert to Calcite's specific one
     * @return a struct type with the input fieldNames, input fieldTypes, and system fields
     */
    public RelDataType buildRelNodeRowType(TableSchema tableSchema) {
        LogicalType[] logicalTypes =
                Arrays.stream(tableSchema.getFieldDataTypes())
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        return buildRelNodeRowType(tableSchema.getFieldNames(), logicalTypes);
    }

    /**
     * Creates a table row type with the given field names and field types. Table row type is table
     * schema for Calcite [[RelNode]]. See [[RelNode#getRowType]].
     *
     * <p>It uses [[StructKind#FULLY_QUALIFIED]] to let each field must be referenced explicitly.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's [[LogicalType]]
     * @return a table row type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildRelNodeRowType(List<String> fieldNames, List<LogicalType> fieldTypes) {
        return buildStructType(fieldNames, fieldTypes, StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a table row type with the input fieldNames and input fieldTypes using
     * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
     * [[RelNode]]. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's [[LogicalType]]
     * @return a table row type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildRelNodeRowType(String[] fieldNames, LogicalType[] fieldTypes) {
        return buildStructType(
                Arrays.asList(fieldNames), Arrays.asList(fieldTypes), StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a table row type with the input fieldNames and input fieldTypes using
     * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
     * [[RelNode]]. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
     */
    public RelDataType buildRelNodeRowType(RowType rowType) {
        List<RowType.RowField> fields = rowType.getFields();
        return buildStructType(
                fields.stream().map(RowType.RowField::getName).collect(Collectors.toList()),
                fields.stream().map(RowType.RowField::getType).collect(Collectors.toList()),
                StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a struct type with the persisted columns using FlinkTypeFactory.
     *
     * @param tableSchema schema to convert to Calcite's specific one
     * @return a struct type with the input fieldsNames, input fieldTypes.
     */
    public RelDataType buildPersistedRelNodeRowType(TableSchema tableSchema) {
        return buildRelNodeRowType(TableSchemaUtils.getPersistedSchema(tableSchema));
    }

    @Override
    public RelDataType createRawType(String className, String serializerString) {
        final RawType<?> rawType = RawType.restore(classLoader, className, serializerString);
        final RelDataType rawRelDataType = createFieldTypeFromLogicalType(rawType);
        return canonize(rawRelDataType);
    }

    @Override
    public java.lang.reflect.Type getJavaClass(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.FLOAT) {
            if (type.isNullable()) {
                return java.lang.Float.class;
            } else {
                return java.lang.Float.TYPE;
            }
        } else {
            return super.getJavaClass(type);
        }
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
            // if we got here, the precision and scale are not specified, here we
            // keep precision/scale in sync with our type system's default value,
            // see DecimalType.USER_DEFAULT.
            return createSqlType(
                    typeName, DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
        } else {
            return super.createSqlType(typeName);
        }
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
        // Calcite will limit the length of the VARCHAR type to 65536.
        if (typeName == SqlTypeName.VARCHAR && precision < 0) {
            return createSqlType(typeName, getTypeSystem().getDefaultPrecision(typeName));
        } else {
            return super.createSqlType(typeName, precision);
        }
    }

    @Override
    public RelDataType createArrayType(RelDataType elementType, long maxCardinality) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(elementType);
        toLogicalType(elementType);
        return super.createArrayType(elementType, maxCardinality);
    }

    @Override
    public RelDataType createMapType(RelDataType keyType, RelDataType valueType) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(keyType, valueType);
        toLogicalType(keyType);
        toLogicalType(valueType);
        return super.createMapType(keyType, valueType);
    }

    @Override
    public RelDataType createMultisetType(RelDataType elementType, long maxCardinality) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(elementType);
        toLogicalType(elementType);
        return super.createMultisetType(elementType, maxCardinality);
    }

    @Override
    public RelDataType createStructuredType(
            String className, List<RelDataType> fieldTypes, List<String> fieldNames) {
        final Optional<Class<?>> resolvedClass =
                StructuredType.resolveClass(classLoader, className);
        final StructuredType.Builder builder =
                resolvedClass
                        .map(StructuredType::newBuilder)
                        .orElseGet(() -> StructuredType.newBuilder(className));

        final List<RelDataTypeField> relFields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            relFields.add(new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldTypes.get(i)));
        }

        List<StructuredType.StructuredAttribute> attributes =
                relFields.stream()
                        .map(
                                f ->
                                        new StructuredType.StructuredAttribute(
                                                f.getName(), toLogicalType(f.getType())))
                        .collect(Collectors.toList());
        builder.attributes(attributes);

        final RelDataType relDataType = new StructuredRelDataType(builder.build(), relFields);
        return canonize(relDataType);
    }

    public Charset getDefaultCharset() {
        return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        final RelDataType leastRestrictive =
                resolveAllIdenticalTypes(types).orElseGet(() -> super.leastRestrictive(types));
        // NULL is reserved for untyped literals only
        if (leastRestrictive == null || leastRestrictive.getSqlTypeName() == SqlTypeName.NULL) {
            return null;
        } else {
            return leastRestrictive;
        }
    }

    public static LogicalType toLogicalType(RelDataType relDataType) {
        return toLogicalTypeWithoutNullability(relDataType).copy(relDataType.isNullable());
    }

    private static LogicalType toLogicalTypeWithoutNullability(RelDataType relDataType) {
        final SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
        switch (sqlTypeName) {
            case BOOLEAN:
                return new BooleanType();
            case TINYINT:
                return new TinyIntType();
            case SMALLINT:
                return new SmallIntType();
            case INTEGER:
                return new IntType();
            case BIGINT:
                return new BigIntType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case CHAR:
                if (relDataType.getPrecision() == 0) {
                    return CharType.ofEmptyLiteral();
                } else {
                    return new CharType(relDataType.getPrecision());
                }
            case VARCHAR:
                if (relDataType.getPrecision() == 0) {
                    return VarCharType.ofEmptyLiteral();
                } else {
                    return new VarCharType(relDataType.getPrecision());
                }
            case BINARY:
                if (relDataType.getPrecision() == 0) {
                    return BinaryType.ofEmptyLiteral();
                } else {
                    return new BinaryType(relDataType.getPrecision());
                }
            case VARBINARY:
                if (relDataType.getPrecision() == 0) {
                    return VarBinaryType.ofEmptyLiteral();
                } else {
                    return new VarBinaryType(relDataType.getPrecision());
                }
            case DECIMAL:
                return new DecimalType(relDataType.getPrecision(), relDataType.getScale());

            // time indicators
            case TIMESTAMP:
                if (relDataType instanceof TimeIndicatorRelDataType) {
                    final TimeIndicatorRelDataType indicator =
                            (TimeIndicatorRelDataType) relDataType;
                    if (indicator.isEventTime()) {
                        return new TimestampType(true, TimestampKind.ROWTIME, 3);
                    } else {
                        throw new TableException(
                                "Processing time indicator only supports"
                                        + " LocalZonedTimestampType, but actual is TimestampType."
                                        + " This is a bug in planner, please file an issue.");
                    }
                } else {
                    return new TimestampType(relDataType.getPrecision());
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (relDataType instanceof TimeIndicatorRelDataType) {
                    final TimeIndicatorRelDataType indicator =
                            (TimeIndicatorRelDataType) relDataType;
                    if (indicator.isEventTime()) {
                        return new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 3);
                    } else {
                        return new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3);
                    }
                } else {
                    return new LocalZonedTimestampType(relDataType.getPrecision());
                }

            // temporal types
            case DATE:
                return new DateType();
            case TIME:
                if (relDataType.getPrecision() > 3) {
                    throw new TableException(
                            "TIME precision is not supported: " + relDataType.getPrecision());
                }
                // the planner supports precision 3, but for consistency with old planner, we set it
                // to 0.
                return new TimeType();

            case NULL:
                return new NullType();

            case SYMBOL:
                return new SymbolType();

            case COLUMN_LIST:
                return new DescriptorType();

            // extract encapsulated Type
            case ANY:
                if (relDataType instanceof GenericRelDataType) {
                    final GenericRelDataType genericRelDataType = (GenericRelDataType) relDataType;
                    return genericRelDataType.genericType();
                } else {
                    throw new TableException("Type is not supported for ANY: " + relDataType);
                }

            case ROW:
                if (relDataType instanceof RelRecordType) {
                    return toLogicalRowType(relDataType);
                } else {
                    throw new TableException("Type is not supported for ROW: " + relDataType);
                }

            case STRUCTURED:
                if (relDataType instanceof StructuredRelDataType) {
                    return ((StructuredRelDataType) relDataType).getStructuredType();
                } else {
                    throw new TableException(
                            "Type is not supported for STRUCTURED: " + relDataType);
                }

            case MULTISET:
                return new MultisetType(toLogicalType(relDataType.getComponentType()));

            case ARRAY:
                return new ArrayType(toLogicalType(relDataType.getComponentType()));

            case MAP:
                if (relDataType instanceof MapSqlType) {
                    final MapSqlType mapRelDataType = (MapSqlType) relDataType;
                    return new MapType(
                            toLogicalType(mapRelDataType.getKeyType()),
                            toLogicalType(mapRelDataType.getValueType()));
                } else {
                    throw new TableException("Type is not supported for MAP: " + relDataType);
                }

            // CURSOR for UDTF case, whose type info will never be used, just a placeholder
            case CURSOR:
                return new TypeInformationRawType(new NothingTypeInfo());

            case VARIANT:
                return new VariantType();

            case OTHER:
                if (relDataType instanceof RawRelDataType) {
                    return ((RawRelDataType) relDataType).getRawType();
                } else {
                    throw new TableException("Type is not supported for OTHER: " + relDataType);
                }

            default:
                if (YEAR_INTERVAL_TYPES.contains(sqlTypeName)) {
                    return DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType();
                } else if (DAY_INTERVAL_TYPES.contains(sqlTypeName)) {
                    if (relDataType.getPrecision() > 3) {
                        throw new TableException(
                                "DAY_INTERVAL_TYPES precision is not supported: "
                                        + relDataType.getPrecision());
                    }
                    return DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType();
                } else {
                    throw new TableException("Type is not supported: " + sqlTypeName);
                }
        }
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType relDataType, boolean isNullable) {

        // nullability change not necessary
        if (relDataType.isNullable() == isNullable) {
            return canonize(relDataType);
        }

        // change nullability
        final RelDataType newType;
        if (relDataType instanceof RawRelDataType) {
            newType = ((RawRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof StructuredRelDataType) {
            newType = ((StructuredRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof GenericRelDataType) {
            newType =
                    new GenericRelDataType(
                            ((GenericRelDataType) relDataType).genericType(),
                            isNullable,
                            typeSystem);
        } else if (relDataType instanceof TimeIndicatorRelDataType) {
            TimeIndicatorRelDataType it = (TimeIndicatorRelDataType) relDataType;
            newType =
                    new TimeIndicatorRelDataType(
                            it.typeSystemField(), it.originalType(), isNullable, it.isEventTime());
            // for nested rows we keep the nullability property,
            // top-level rows fall back to Calcite's default handling
        } else if (relDataType instanceof RelRecordType
                && relDataType.getStructKind() == StructKind.PEEK_FIELDS_NO_EXPAND) {
            RelRecordType rt = (RelRecordType) relDataType;
            newType = new RelRecordType(rt.getStructKind(), rt.getFieldList(), isNullable);
        } else {
            newType = super.createTypeWithNullability(relDataType, isNullable);
        }

        return canonize(newType);
    }

    /**
     * Create a calcite field type in table schema from [[LogicalType]]. It use
     * PEEK_FIELDS_NO_EXPAND when type is a nested struct type (Flink [[RowType]]).
     *
     * @param t flink logical type.
     * @return calcite [[RelDataType]].
     */
    public RelDataType createFieldTypeFromLogicalType(LogicalType t) {

        final RelDataType relType = createTypeFromLogicalType(t);

        return createTypeWithNullability(relType, t.isNullable());
    }

    private RelDataType createTypeFromLogicalType(LogicalType t) {
        // Kind in TimestampType do not affect the hashcode and equals, So we can't put it to
        // seenTypes
        switch (t.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) t;
                switch (timestampType.getKind()) {
                    case ROWTIME:
                        return createRowtimeIndicatorType(t.isNullable(), false);
                    case REGULAR:
                        return createSqlType(SqlTypeName.TIMESTAMP, timestampType.getPrecision());
                    case PROCTIME:
                        throw new TableException(
                                "Processing time indicator only supports"
                                        + " LocalZonedTimestampType, but actual is TimestampType."
                                        + " This is a bug in planner, please file an issue.");
                    default:
                        throw new TableException(
                                "Unsupported TimestampType kind " + timestampType.getKind());
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType lzTs = (LocalZonedTimestampType) t;
                switch (lzTs.getKind()) {
                    case PROCTIME:
                        return createProctimeIndicatorType(t.isNullable());
                    case ROWTIME:
                        return createRowtimeIndicatorType(t.isNullable(), true);
                    case REGULAR:
                        return createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, lzTs.getPrecision());
                    default:
                        throw new TableException(
                                "Unsupported LocalZonedTimestampType kind " + lzTs.getKind());
                }
            default:
                if (seenTypes.containsKey(t)) {
                    return seenTypes.get(t);
                } else {
                    final RelDataType refType = newRelDataType(t);
                    seenTypes.put(t, refType);
                    return refType;
                }
        }
    }

    private RelDataType newRelDataType(LogicalType t) {
        switch (t.getTypeRoot()) {
            case NULL:
                return createSqlType(SqlTypeName.NULL);
            case BOOLEAN:
                return createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return createSqlType(SqlTypeName.SMALLINT);
            case INTEGER:
                return createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return createSqlType(SqlTypeName.DOUBLE);
            case VARCHAR:
                return createSqlType(SqlTypeName.VARCHAR, ((VarCharType) t).getLength());
            case CHAR:
                return createSqlType(SqlTypeName.CHAR, ((CharType) t).getLength());

            // temporal types
            case DATE:
                return createSqlType(SqlTypeName.DATE);
            case TIME_WITHOUT_TIME_ZONE:
                return createSqlType(SqlTypeName.TIME);

            // interval types
            case INTERVAL_YEAR_MONTH:
                return createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
            case INTERVAL_DAY_TIME:
                return createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));

            case BINARY:
                return createSqlType(SqlTypeName.BINARY, ((BinaryType) t).getLength());
            case VARBINARY:
                return createSqlType(SqlTypeName.VARBINARY, ((VarBinaryType) t).getLength());

            case DECIMAL:
                if (t instanceof DecimalType) {
                    final DecimalType decimalType = (DecimalType) t;
                    return createSqlType(
                            SqlTypeName.DECIMAL,
                            decimalType.getPrecision(),
                            decimalType.getScale());
                } else if (t instanceof LegacyTypeInformationType) {
                    final LegacyTypeInformationType legacyType = (LegacyTypeInformationType) t;
                    if (legacyType.getTypeInformation() == BasicTypeInfo.BIG_DEC_TYPE_INFO) {
                        return createSqlType(SqlTypeName.DECIMAL, 38, 18);
                    } else {
                        throw new TableException(
                                "Type information is not supported for DECIMAL: "
                                        + legacyType.getTypeInformation());
                    }
                } else {
                    throw new TableException("Type is not supported for DECIMAL: " + t);
                }

            case ROW:
                final RowType rowType = (RowType) t;
                return buildStructType(
                        rowType.getFieldNames(),
                        rowType.getChildren(),
                        // fields are not expanded in "SELECT *"
                        StructKind.PEEK_FIELDS_NO_EXPAND);

            case STRUCTURED_TYPE:
                if (t instanceof StructuredType) {
                    return StructuredRelDataType.create(
                            (FlinkTypeFactory) this, (StructuredType) t);
                } else if (t instanceof LegacyTypeInformationType) {
                    return createFieldTypeFromLogicalType(PlannerTypeUtils.removeLegacyTypes(t));
                } else {
                    throw new TableException("Type is not supported for STRUCTURED_TYPE: " + t);
                }

            case ARRAY:
                final ArrayType arrayType = (ArrayType) t;
                return createArrayType(
                        createFieldTypeFromLogicalType(arrayType.getElementType()), -1);

            case MAP:
                final MapType mapType = (MapType) t;
                return createMapType(
                        createFieldTypeFromLogicalType(mapType.getKeyType()),
                        createFieldTypeFromLogicalType(mapType.getValueType()));

            case MULTISET:
                final MultisetType multisetType = (MultisetType) t;
                return createMultisetType(
                        createFieldTypeFromLogicalType(multisetType.getElementType()), -1);

            case RAW:
                if (t instanceof RawType) {
                    return new RawRelDataType((RawType<?>) t);
                } else if (t instanceof TypeInformationRawType) {
                    return new GenericRelDataType(
                            (TypeInformationRawType<?>) t, true, getTypeSystem());
                } else if (t instanceof LegacyTypeInformationType) {
                    return createFieldTypeFromLogicalType(PlannerTypeUtils.removeLegacyTypes(t));
                } else {
                    throw new TableException("Type is not supported for RAW: " + t);
                }

            case SYMBOL:
                return createSqlType(SqlTypeName.SYMBOL);

            case DESCRIPTOR:
                return createSqlType(SqlTypeName.COLUMN_LIST);

            case VARIANT:
                return createSqlType(SqlTypeName.VARIANT);
            default:
                throw new TableException("Type is not supported: " + t);
        }
    }

    /** Creates a indicator type for event-time, but with similar properties as SQL timestamp. */
    public RelDataType createRowtimeIndicatorType(boolean isNullable, boolean isTimestampLtz) {
        final RelDataType originalType =
                (isTimestampLtz)
                        ? createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3))
                        : createFieldTypeFromLogicalType(new TimestampType(isNullable, 3));

        return canonize(
                new TimeIndicatorRelDataType(
                        getTypeSystem(), (BasicSqlType) originalType, isNullable, true));
    }

    /**
     * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
     */
    public RelDataType createProctimeIndicatorType(boolean isNullable) {
        final RelDataType originalType =
                createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3));
        return canonize(
                new TimeIndicatorRelDataType(
                        getTypeSystem(), (BasicSqlType) originalType, isNullable, false));
    }

    public static RowType toLogicalRowType(RelDataType relType) {
        checkArgument(relType.isStruct());
        return RowType.of(
                relType.getFieldList().stream()
                        .map(fieldType -> toLogicalType(fieldType.getType()))
                        .toArray(LogicalType[]::new),
                relType.getFieldNames().toArray(new String[0]));
    }

    /**
     * This is a safety check in case the null type ends up in the type factory for other use cases
     * than untyped NULL literals.
     */
    protected void checkForNullType(RelDataType... childTypes) {
        for (RelDataType relDataType : childTypes) {
            if (relDataType.getSqlTypeName() == SqlTypeName.NULL) {
                throw new ValidationException(
                        "The null type is reserved for representing untyped NULL literals. It should not be "
                                + "used in constructed types. Please cast NULL literals to a more explicit type.");
            }
        }
    }

    protected Optional<RelDataType> resolveAllIdenticalTypes(List<RelDataType> types) {
        if (types.isEmpty()) {
            return Optional.empty();
        }
        RelDataType head = types.get(0);

        // check if all types are the same
        if (types.stream().allMatch(head::equals)) {
            // types are the same, check nullability
            final boolean nullable = head.isNullable() || head.getSqlTypeName() == SqlTypeName.NULL;
            // return type with nullability
            return Optional.of(createTypeWithNullability(head, nullable));
        } else {
            // types are not all the same
            if (types.stream().anyMatch(t -> t.getSqlTypeName() == SqlTypeName.ANY)) {
                // one of the type was RAW.
                // we cannot generate a common type if it differs from other types.
                throw new TableException("Generic RAW types must have a common type information.");
            } else {
                // cannot resolve a common type for different input types
                return Optional.empty();
            }
        }
    }

    /**
     * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's [[LogicalType]].
     * @param structKind Name resolution policy. See more information in [[StructKind]].
     * @return a struct type with the input fieldNames, input fieldTypes.
     */
    private RelDataType buildStructType(
            List<String> fieldNames, List<LogicalType> fieldTypes, StructKind structKind) {
        final FieldInfoBuilder b = builder();
        b.kind(structKind);
        for (int i = 0; i < fieldTypes.size(); i++) {
            final RelDataType fieldRelDataType = createFieldTypeFromLogicalType(fieldTypes.get(i));
            checkForNullType(fieldRelDataType);
            b.add(fieldNames.get(i), fieldRelDataType);
        }
        return b.build();
    }

    public static boolean isTimeIndicatorType(LogicalType t) {
        if (t instanceof TimestampType) {
            return ((TimestampType) t).getKind() == TimestampKind.ROWTIME;
        } else if (t instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) t).getKind() == TimestampKind.PROCTIME;
        }
        return false;
    }

    public static boolean isTimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType;
    }

    public static boolean isRowtimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && ((TimeIndicatorRelDataType) relDataType).isEventTime();
    }

    public static boolean isProctimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && !((TimeIndicatorRelDataType) relDataType).isEventTime();
    }

    public static boolean isTimestampLtzIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && ((TimeIndicatorRelDataType) relDataType)
                        .originalType()
                        .getSqlTypeName()
                        .equals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    public static boolean isProctimeIndicatorType(TypeInformation typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo
                && !((TimeIndicatorTypeInfo) typeInfo).isEventTime();
    }

    @Deprecated
    public static boolean isRowtimeIndicatorType(TypeInformation typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo
                && ((TimeIndicatorTypeInfo) typeInfo).isEventTime();
    }

    @Deprecated
    public static boolean isTimeIndicatorType(TypeInformation typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo;
    }

    public static TableSchema toTableSchema(RelDataType relDataType) {
        String[] fieldNames = relDataType.getFieldNames().toArray(new String[0]);
        DataType[] fieldTypes =
                relDataType.getFieldList().stream()
                        .map(
                                f ->
                                        LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
                                                toLogicalType(f.getType())))
                        .toArray(DataType[]::new);
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    /** Returns a projected [[RelDataType]] of the structure type. */
    public RelDataType projectStructType(RelDataType relType, int[] selectedFields) {
        return this.createStructType(
                Arrays.stream(selectedFields)
                        .boxed()
                        .map(
                                idx -> {
                                    RelDataTypeField field = relType.getFieldList().get(idx);
                                    return Map.entry(field.getName(), field.getType());
                                })
                        .collect(Collectors.toList()));
    }
}
