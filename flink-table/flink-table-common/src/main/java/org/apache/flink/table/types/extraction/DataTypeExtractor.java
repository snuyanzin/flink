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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.AvroUtils;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.DataView;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.ExtractionUtils.collectStructuredFields;
import static org.apache.flink.table.types.extraction.ExtractionUtils.collectTypeHierarchy;
import static org.apache.flink.table.types.extraction.ExtractionUtils.createRawType;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractAssigningConstructor;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.ExtractionUtils.hasInvokableConstructor;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isStructuredFieldMutable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.resolveVariable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.resolveVariableWithClassContext;
import static org.apache.flink.table.types.extraction.ExtractionUtils.toClass;
import static org.apache.flink.table.types.extraction.ExtractionUtils.validateStructuredClass;
import static org.apache.flink.table.types.extraction.ExtractionUtils.validateStructuredFieldReadability;
import static org.apache.flink.table.types.extraction.ExtractionUtils.validateStructuredSelfReference;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

/**
 * Reflection-based utility that analyzes a given {@link java.lang.reflect.Type}, method, or class
 * to extract a (possibly nested) {@link DataType} from it.
 */
@Internal
public final class DataTypeExtractor {

    private static final Set<Class<?>> INTERNAL_DATA_STRUCTURES = new HashSet<>();

    static {
        INTERNAL_DATA_STRUCTURES.add(RowData.class);
        INTERNAL_DATA_STRUCTURES.add(StringData.class);
        INTERNAL_DATA_STRUCTURES.add(TimestampData.class);
        INTERNAL_DATA_STRUCTURES.add(DecimalData.class);
        INTERNAL_DATA_STRUCTURES.add(ArrayData.class);
        INTERNAL_DATA_STRUCTURES.add(MapData.class);
        INTERNAL_DATA_STRUCTURES.add(RawValueData.class);
    }

    private final DataTypeFactory typeFactory;

    private final String contextExplanation;

    private DataTypeExtractor(DataTypeFactory typeFactory, String contextExplanation) {
        this.typeFactory = typeFactory;
        this.contextExplanation = contextExplanation;
    }

    // --------------------------------------------------------------------------------------------
    // Methods that extract a data type from a JVM Type without any prior information
    // --------------------------------------------------------------------------------------------

    /** Extracts a data type from a type without considering surrounding classes or templates. */
    public static DataType extractFromType(DataTypeFactory typeFactory, Type type) {
        return extractDataTypeWithClassContext(
                typeFactory, DataTypeTemplate.fromDefaults(), null, type, "");
    }

    /** Extracts a data type from a type without considering surrounding classes but templates. */
    static DataType extractFromType(
            DataTypeFactory typeFactory, DataTypeTemplate template, Type type) {
        return extractDataTypeWithClassContext(typeFactory, template, null, type, "");
    }

    /**
     * Extracts a data type from a type variable at {@code genericPos} of {@code baseClass} using
     * the information of the most specific type {@code contextType}.
     */
    public static DataType extractFromGeneric(
            DataTypeFactory typeFactory, Class<?> baseClass, int genericPos, Type contextType) {
        final TypeVariable<?> variable = baseClass.getTypeParameters()[genericPos];
        return extractDataTypeWithClassContext(
                typeFactory,
                DataTypeTemplate.fromDefaults(),
                contextType,
                variable,
                String.format(
                        " in generic class '%s' in %s",
                        baseClass.getName(), contextType.toString()));
    }

    /**
     * Extracts a data type from a method parameter by considering surrounding classes and parameter
     * annotation.
     */
    public static DataType extractFromMethodParameter(
            DataTypeFactory typeFactory, Class<?> baseClass, Method method, int paramPos) {
        final Parameter parameter = method.getParameters()[paramPos];
        final DataTypeHint hint = parameter.getAnnotation(DataTypeHint.class);
        final ArgumentHint argumentHint = parameter.getAnnotation(ArgumentHint.class);
        final StateHint stateHint = parameter.getAnnotation(StateHint.class);
        final DataTypeTemplate template;
        if (stateHint != null) {
            template = DataTypeTemplate.fromAnnotation(typeFactory, stateHint.type());
        } else if (argumentHint != null) {
            template = DataTypeTemplate.fromAnnotation(typeFactory, argumentHint.type());
        } else if (hint != null) {
            template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
        } else {
            template = DataTypeTemplate.fromDefaults();
        }
        return extractDataTypeWithClassContext(
                typeFactory,
                template,
                baseClass,
                parameter.getParameterizedType(),
                String.format(
                        " in parameter %d of method '%s' in class '%s'",
                        paramPos, method.getName(), baseClass.getName()));
    }

    /**
     * Extracts a data type from a method parameter by considering surrounding classes and parameter
     * annotation. This version assumes that the parameter is a generic type, and uses the generic
     * position type as the extracted data type. For example, if the parameter is a
     * CompletableFuture&lt;Long&gt; and genericPos is 0, it will extract Long.
     */
    public static DataType extractFromGenericMethodParameter(
            DataTypeFactory typeFactory,
            Class<?> baseClass,
            Method method,
            int paramPos,
            int genericPos) {

        Type parameterType = method.getGenericParameterTypes()[paramPos];
        parameterType = resolveVariableWithClassContext(baseClass, parameterType);
        if (!(parameterType instanceof ParameterizedType)) {
            throw extractionError(
                    "The method '%s' needs generic parameters for the %d arg.",
                    method.getName(), paramPos);
        }
        final Type genericParameterType =
                ((ParameterizedType) parameterType).getActualTypeArguments()[genericPos];
        final Parameter parameter = method.getParameters()[paramPos];
        final DataTypeHint hint = parameter.getAnnotation(DataTypeHint.class);
        final DataTypeTemplate template;
        if (hint != null) {
            template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
        } else {
            template = DataTypeTemplate.fromDefaults();
        }
        return extractDataTypeWithClassContext(
                typeFactory,
                template,
                baseClass,
                genericParameterType,
                String.format(
                        " in generic parameter %d of method '%s' in class '%s'",
                        paramPos, method.getName(), baseClass.getName()));
    }

    /**
     * Extracts a data type from a method return type by considering surrounding classes and method
     * annotation.
     */
    public static DataType extractFromMethodReturnType(
            DataTypeFactory typeFactory, Class<?> baseClass, Method method) {
        return extractFromMethodReturnType(
                typeFactory, baseClass, method, method.getGenericReturnType());
    }

    /**
     * Extracts a data type from a method return type with specifying the method's type explicitly
     * by considering surrounding classes and method annotation.
     */
    public static DataType extractFromMethodReturnType(
            DataTypeFactory typeFactory, Class<?> baseClass, Method method, Type methodReturnType) {
        final DataTypeHint hint = method.getAnnotation(DataTypeHint.class);
        final DataTypeTemplate template;
        if (hint != null) {
            template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
        } else {
            template = DataTypeTemplate.fromDefaults();
        }
        return extractDataTypeWithClassContext(
                typeFactory,
                template,
                baseClass,
                methodReturnType,
                String.format(
                        " in return type of method '%s' in class '%s'",
                        method.getName(), baseClass.getName()));
    }

    // --------------------------------------------------------------------------------------------
    // Methods that extract a data type from a JVM Class with prior logical information
    // --------------------------------------------------------------------------------------------

    public static DataType extractFromStructuredClass(
            DataTypeFactory typeFactory, Class<?> implementationClass) {
        final DataType dataType =
                extractDataTypeWithClassContext(
                        typeFactory,
                        DataTypeTemplate.fromDefaults(),
                        implementationClass.getEnclosingClass(),
                        implementationClass,
                        "");
        if (!dataType.getLogicalType().is(LogicalTypeRoot.STRUCTURED_TYPE)) {
            throw extractionError(
                    "Structured data type expected for class '%s' but was: %s",
                    implementationClass.getName(), dataType);
        }
        return dataType;
    }

    // --------------------------------------------------------------------------------------------
    // Supporting methods
    // --------------------------------------------------------------------------------------------

    private static DataType extractDataTypeWithClassContext(
            DataTypeFactory typeFactory,
            DataTypeTemplate outerTemplate,
            @Nullable Type contextType,
            Type type,
            String contextExplanation) {
        final DataTypeExtractor extractor = new DataTypeExtractor(typeFactory, contextExplanation);
        final List<Type> typeHierarchy;
        if (contextType != null) {
            typeHierarchy = collectTypeHierarchy(contextType);
        } else {
            typeHierarchy = Collections.emptyList();
        }
        return extractor.extractDataTypeOrRaw(outerTemplate, typeHierarchy, type);
    }

    private DataType extractDataTypeOrRaw(
            DataTypeTemplate outerTemplate, List<Type> typeHierarchy, Type type) {
        // best effort resolution of type variables, the resolved type can still be a variable
        final Type resolvedType;
        if (type instanceof TypeVariable) {
            resolvedType = resolveVariable(typeHierarchy, (TypeVariable<?>) type);
        } else {
            resolvedType = type;
        }
        // merge outer template with template of type itself
        DataTypeTemplate template = outerTemplate;
        final Class<?> clazz = toClass(resolvedType);
        if (clazz != null) {
            final DataTypeHint hint = clazz.getAnnotation(DataTypeHint.class);
            final ArgumentHint argumentHint = clazz.getAnnotation(ArgumentHint.class);
            if (hint != null) {
                template = outerTemplate.mergeWithInnerAnnotation(typeFactory, hint);
            } else if (argumentHint != null) {
                template = outerTemplate.mergeWithInnerAnnotation(typeFactory, argumentHint.type());
            }
        }
        // main work
        DataType dataType = extractDataTypeOrRawWithTemplate(template, typeHierarchy, resolvedType);
        // handle data views
        dataType = handleDataViewHints(dataType, clazz);
        // final work
        return closestBridging(dataType, clazz);
    }

    private DataType extractDataTypeOrRawWithTemplate(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
        // template defines a data type
        if (template.dataType != null) {
            return template.dataType;
        }
        try {
            return extractDataTypeOrError(template, typeHierarchy, type);
        } catch (Throwable t) {
            // ignore the exception and just treat it as RAW type
            final Class<?> clazz = toClass(type);
            if (template.isAllowRawGlobally() || template.isAllowAnyPattern(clazz)) {
                return createRawType(typeFactory, template.rawSerializer, clazz);
            }
            // forward the root cause otherwise
            throw extractionError(
                    t,
                    "Could not extract a data type from '%s'%s. "
                            + "Please pass the required data type manually or allow RAW types.",
                    type.toString(),
                    contextExplanation);
        }
    }

    private DataType extractDataTypeOrError(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
        // still a type variable
        if (type instanceof TypeVariable) {
            throw extractionError(
                    "Unresolved type variable '%s'. A data type cannot be extracted from a type variable. "
                            + "The original content might have been erased due to Java type erasure.",
                    type.toString());
        }

        // ARRAY
        DataType resultDataType = extractArrayType(template, typeHierarchy, type);
        if (resultDataType != null) {
            return resultDataType;
        }

        // skip extraction for enforced patterns early but after arrays
        resultDataType = extractEnforcedRawType(template, type);
        if (resultDataType != null) {
            return resultDataType;
        }

        // early and helpful exception for common mistakes
        checkForCommonErrors(type);

        // PREDEFINED or DESCRIPTOR
        resultDataType = extractPredefinedOrDescriptorType(template, type);
        if (resultDataType != null) {
            return resultDataType;
        }

        // MAP
        resultDataType = extractMapType(template, typeHierarchy, type);
        if (resultDataType != null) {
            return resultDataType;
        }

        // AVRO
        resultDataType = extractAvroType(type);
        if (resultDataType != null) {
            return resultDataType;
        }

        // try interpret the type as a STRUCTURED type
        try {
            return extractStructuredType(template, typeHierarchy, type);
        } catch (Throwable t) {
            throw extractionError(
                    t,
                    "Could not extract a data type from '%s'. "
                            + "Interpreting it as a structured type was also not successful.",
                    type.toString());
        }
    }

    private @Nullable DataType extractArrayType(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
        // prefer BYTES over ARRAY<TINYINT> for byte[]
        if (type == byte[].class) {
            return DataTypes.BYTES();
        }
        // for T[]
        else if (type instanceof GenericArrayType) {
            final GenericArrayType genericArray = (GenericArrayType) type;
            return DataTypes.ARRAY(
                    extractDataTypeOrRaw(
                            template, typeHierarchy, genericArray.getGenericComponentType()));
        }

        final Class<?> clazz = toClass(type);
        if (clazz == null) {
            return null;
        }

        // for my.custom.Pojo[][]
        if (clazz.isArray()) {
            return DataTypes.ARRAY(
                    extractDataTypeOrRaw(template, typeHierarchy, clazz.getComponentType()));
        }

        // for List<T>
        // we only allow List here (not a subclass) because we cannot guarantee more specific
        // data structures after conversion
        if (clazz != List.class) {
            return null;
        }
        if (!(type instanceof ParameterizedType)) {
            throw extractionError(
                    "The class '%s' needs generic parameters for an array type.",
                    List.class.getName());
        }
        final ParameterizedType parameterizedType = (ParameterizedType) type;
        final DataType element =
                extractDataTypeOrRaw(
                        template, typeHierarchy, parameterizedType.getActualTypeArguments()[0]);
        return DataTypes.ARRAY(element).bridgedTo(List.class);
    }

    private @Nullable DataType extractEnforcedRawType(DataTypeTemplate template, Type type) {
        final Class<?> clazz = toClass(type);
        if (template.isForceAnyPattern(clazz)) {
            return createRawType(typeFactory, template.rawSerializer, clazz);
        }
        return null;
    }

    private void checkForCommonErrors(Type type) {
        final Class<?> clazz = toClass(type);
        if (clazz == null) {
            return;
        }

        if (clazz == Row.class) {
            throw extractionError(
                    "Cannot extract a data type from a pure '%s' class. "
                            + "Please use annotations to define field names and field types.",
                    Row.class.getName());
        } else if (clazz == Object.class) {
            throw extractionError(
                    "Cannot extract a data type from a pure '%s' class. "
                            + "Usually, this indicates that class information is missing or got lost. "
                            + "Please specify a more concrete class or treat it as a RAW type.",
                    Object.class.getName());
        } else if (INTERNAL_DATA_STRUCTURES.contains(clazz)) {
            throw extractionError(
                    "Cannot extract a data type from an internal '%s' class without further information. "
                            + "Please use annotations to define the full logical type.",
                    clazz.getName());
        } else if (clazz.getName().startsWith("scala.Tuple")) {
            throw extractionError(
                    "Scala tuples are not supported. Use case classes or '%s' instead.",
                    Row.class.getName());
        } else if (clazz.getName().startsWith("scala.collection")) {
            throw extractionError(
                    "Scala collections are not supported. "
                            + "See the documentation for supported classes or treat them as RAW types.");
        }
    }

    private @Nullable DataType extractPredefinedOrDescriptorType(
            DataTypeTemplate template, Type type) {
        final Class<?> clazz = toClass(type);
        // all predefined types are representable as classes
        if (clazz == null) {
            return null;
        }

        // DECIMAL
        if (clazz == BigDecimal.class) {
            if (template.defaultDecimalPrecision != null && template.defaultDecimalScale != null) {
                return DataTypes.DECIMAL(
                        template.defaultDecimalPrecision, template.defaultDecimalScale);
            } else if (template.defaultDecimalPrecision != null) {
                return DataTypes.DECIMAL(template.defaultDecimalPrecision, 0);
            }
            throw extractionError(
                    "Values of '%s' need fixed precision and scale.", BigDecimal.class.getName());
        }

        // TIME
        else if (clazz == java.sql.Time.class || clazz == java.time.LocalTime.class) {
            if (template.defaultSecondPrecision != null) {
                return DataTypes.TIME(template.defaultSecondPrecision).bridgedTo(clazz);
            }
        }

        // TIMESTAMP
        else if (clazz == java.sql.Timestamp.class || clazz == java.time.LocalDateTime.class) {
            if (template.defaultSecondPrecision != null) {
                return DataTypes.TIMESTAMP(template.defaultSecondPrecision).bridgedTo(clazz);
            }
        }

        // TIMESTAMP WITH TIME ZONE
        else if (clazz == java.time.OffsetDateTime.class) {
            if (template.defaultSecondPrecision != null) {
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE(template.defaultSecondPrecision);
            }
        }

        // TIMESTAMP WITH LOCAL TIME ZONE
        else if (clazz == java.time.Instant.class) {
            if (template.defaultSecondPrecision != null) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(template.defaultSecondPrecision);
            }
        }

        // INTERVAL SECOND
        else if (clazz == java.time.Duration.class) {
            if (template.defaultSecondPrecision != null) {
                return DataTypes.INTERVAL(DataTypes.SECOND(template.defaultSecondPrecision));
            }
        }

        // INTERVAL YEAR TO MONTH
        else if (clazz == java.time.Period.class) {
            if (template.defaultYearPrecision != null && template.defaultYearPrecision == 0) {
                return DataTypes.INTERVAL(DataTypes.MONTH());
            } else if (template.defaultYearPrecision != null) {
                return DataTypes.INTERVAL(
                        DataTypes.YEAR(template.defaultYearPrecision), DataTypes.MONTH());
            }
        }

        return ClassDataTypeConverter.extractDataType(clazz).orElse(null);
    }

    private @Nullable DataType extractMapType(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
        final Class<?> clazz = toClass(type);
        // we only allow Map here (not a subclass) because we cannot guarantee more specific
        // data structures after conversion
        if (clazz != Map.class) {
            return null;
        }
        if (!(type instanceof ParameterizedType)) {
            throw extractionError(
                    "The class '%s' needs generic parameters for a map type.", Map.class.getName());
        }
        final ParameterizedType parameterizedType = (ParameterizedType) type;
        final DataType key =
                extractDataTypeOrRaw(
                        template, typeHierarchy, parameterizedType.getActualTypeArguments()[0]);
        final DataType value =
                extractDataTypeOrRaw(
                        template, typeHierarchy, parameterizedType.getActualTypeArguments()[1]);
        return DataTypes.MAP(key, value);
    }

    private @Nullable DataType extractAvroType(Type type) {
        final Class<?> clazz = toClass(type);
        if (AvroUtils.isAvroSpecificRecord(clazz)) {
            // refer to TypeExtractor#privateGetForClass to get the AvroTypeInfo
            return TypeInfoDataTypeConverter.toDataType(
                    typeFactory, AvroUtils.getAvroUtils().createAvroTypeInfo(clazz));
        }
        return null;
    }

    private DataType extractStructuredType(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
        final Class<?> clazz = toClass(type);
        if (clazz == null) {
            throw extractionError("Not a class type.");
        }

        validateStructuredClass(clazz);
        validateStructuredSelfReference(type, typeHierarchy);

        final List<Field> fields = collectStructuredFields(clazz);

        if (fields.isEmpty()) {
            throw extractionError("Class '%s' has no fields.", clazz.getName());
        }

        // if not all fields are mutable, a default constructor is not enough
        final boolean allFieldsMutable =
                fields.stream()
                        .allMatch(
                                f -> {
                                    validateStructuredFieldReadability(clazz, f);
                                    return isStructuredFieldMutable(clazz, f);
                                });

        final ExtractionUtils.AssigningConstructor constructor =
                extractAssigningConstructor(clazz, fields);
        if (!allFieldsMutable && constructor == null) {
            throw extractionError(
                    "Class '%s' has immutable fields and thus requires a constructor that is publicly "
                            + "accessible and assigns all fields: %s",
                    clazz.getName(),
                    fields.stream().map(Field::getName).collect(Collectors.joining(", ")));
        }
        // check for a default constructor otherwise
        else if (constructor == null && !hasInvokableConstructor(clazz)) {
            throw extractionError(
                    "Class '%s' has neither a constructor that assigns all fields nor a default constructor.",
                    clazz.getName());
        }

        final Map<String, DataType> fieldDataTypes =
                extractStructuredTypeFields(template, typeHierarchy, type, fields);

        final DataTypes.Field[] attributes =
                createStructuredTypeAttributes(constructor, fieldDataTypes);

        return DataTypes.STRUCTURED(clazz, attributes);
    }

    private Map<String, DataType> extractStructuredTypeFields(
            DataTypeTemplate template, List<Type> typeHierarchy, Type type, List<Field> fields) {
        final Map<String, DataType> fieldDataTypes = new HashMap<>();
        final List<Type> structuredTypeHierarchy = collectTypeHierarchy(type);
        for (Field field : fields) {
            try {
                final Type fieldType = field.getGenericType();
                final List<Type> fieldTypeHierarchy = new ArrayList<>();
                // hierarchy until structured type
                fieldTypeHierarchy.addAll(typeHierarchy);
                // hierarchy of structured type
                fieldTypeHierarchy.addAll(structuredTypeHierarchy);
                final DataTypeTemplate fieldTemplate =
                        mergeFieldTemplate(typeFactory, field, template);
                final DataType fieldDataType =
                        extractDataTypeOrRaw(fieldTemplate, fieldTypeHierarchy, fieldType);
                fieldDataTypes.put(field.getName(), fieldDataType);
            } catch (Throwable t) {
                throw extractionError(
                        t,
                        "Error in field '%s' of class '%s'.",
                        field.getName(),
                        field.getDeclaringClass().getName());
            }
        }
        return fieldDataTypes;
    }

    private DataTypes.Field[] createStructuredTypeAttributes(
            ExtractionUtils.AssigningConstructor constructor,
            Map<String, DataType> fieldDataTypes) {
        return Optional.ofNullable(constructor)
                .map(
                        c -> {
                            // field order is defined by assigning constructor
                            return c.parameterNames.stream();
                        })
                .orElseGet(
                        () -> {
                            // field order is sorted
                            return fieldDataTypes.keySet().stream().sorted();
                        })
                .map(name -> DataTypes.FIELD(name, fieldDataTypes.get(name)))
                .toArray(DataTypes.Field[]::new);
    }

    /** Merges the template of a structured type with a possibly more specific field annotation. */
    private DataTypeTemplate mergeFieldTemplate(
            DataTypeFactory typeFactory, Field field, DataTypeTemplate structuredTemplate) {
        final DataTypeHint hint = field.getAnnotation(DataTypeHint.class);
        if (hint == null) {
            return structuredTemplate.copyWithoutDataType();
        }
        return structuredTemplate.mergeWithInnerAnnotation(typeFactory, hint);
    }

    /**
     * Use closest class for data type if possible. Even though a hint might have provided some data
     * type, in many cases, the conversion class can be enriched with the extraction type itself.
     */
    private DataType closestBridging(DataType dataType, @Nullable Class<?> clazz) {
        // no context class or conversion class is already more specific than context class
        if (clazz == null || clazz.isAssignableFrom(dataType.getConversionClass())) {
            return dataType;
        }
        final LogicalType logicalType = dataType.getLogicalType();
        final boolean supportsConversion =
                logicalType.supportsInputConversion(clazz)
                        || logicalType.supportsOutputConversion(clazz);
        if (supportsConversion) {
            return dataType.bridgedTo(clazz);
        }
        return dataType;
    }

    /**
     * Data type hints are allowed on top of {@link DataView}s. They are validated and mapped to the
     * underlying collection by this method.
     */
    private DataType handleDataViewHints(DataType dataType, @Nullable Class<?> clazz) {
        if (clazz == null || !DataView.class.isAssignableFrom(clazz)) {
            return dataType;
        }

        // data type went through regular extraction logic
        if (isCompositeType(dataType.getLogicalType())) {
            return dataType;
        }

        // view was annotated
        if (ListView.class.isAssignableFrom(clazz)) {
            if (!dataType.getLogicalType().is(LogicalTypeRoot.ARRAY)) {
                throw extractionError("Annotated list views should have a logical type of ARRAY.");
            }
            final CollectionDataType collectionDataType = (CollectionDataType) dataType;
            return ListView.newListViewDataType(collectionDataType.getElementDataType());
        } else if (MapView.class.isAssignableFrom(clazz)) {
            if (!dataType.getLogicalType().is(LogicalTypeRoot.MAP)) {
                throw extractionError("Annotated map views should have a logical type of MAP.");
            }
            final KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
            return MapView.newMapViewDataType(
                    keyValueDataType.getKeyDataType(), keyValueDataType.getValueDataType());
        } else {
            throw extractionError("Invalid data view: %s", clazz.getName());
        }
    }
}
