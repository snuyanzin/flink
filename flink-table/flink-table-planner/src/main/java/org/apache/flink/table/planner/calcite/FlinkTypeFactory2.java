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

import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.ExtendedRelTypeFactory;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.planner.plan.schema.GenericRelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Flink specific type factory that represents the interface between Flink's [[LogicalType]] and
 * Calcite's [[RelDataType]].
 */
public class FlinkTypeFactory2 extends JavaTypeFactoryImpl implements ExtendedRelTypeFactory {
    final ClassLoader classLoader;
    final Map<LogicalType, RelDataType> seenTypes = new HashMap<>();

    public FlinkTypeFactory2(RelDataTypeSystem typeSystem, ClassLoader classLoader) {
        super(typeSystem);
        this.classLoader = classLoader;
    }

    @Override
    public RelDataType createRawType(String className, String serializerString) {
        return null;
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
}
