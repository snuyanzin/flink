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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.util.Arrays;
import java.util.Optional;

/**
 * Argument strategy for functions taking an array (argument 0) and an element (argument 1), e.g.
 * {@link BuiltInFunctionDefinitions#ARRAY_CONTAINS}. Widens both to the common element type so a
 * wider element (e.g. a higher-precision {@code DECIMAL}) is not narrowed: returns the widened
 * {@link ArrayType} for the array and the common type for the element.
 */
@Internal
class ArrayElementArgumentTypeStrategy implements ArgumentTypeStrategy {

    /** The array argument always precedes the element argument. */
    private static final int ARRAY_POS = 0;

    private static final int ELEMENT_POS = 1;

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        // empty unless arg 0 is an array, so the ArrayType cast below is safe
        return findCommonElementType(callContext)
                .map(
                        commonType -> {
                            final LogicalType argType =
                                    callContext
                                            .getArgumentDataTypes()
                                            .get(argumentPos)
                                            .getLogicalType();
                            if (argumentPos == ARRAY_POS) {
                                final LogicalType arrayElementType =
                                        ((ArrayType) argType).getElementType();
                                return new ArrayType(
                                        argType.isNullable(),
                                        commonType.copy(arrayElementType.isNullable()));
                            }
                            return commonType.copy(argType.isNullable());
                        })
                .map(DataTypes::of);
    }

    /**
     * Common type of the array element and the element argument; empty if arg 0 is not an array.
     */
    static Optional<LogicalType> findCommonElementType(CallContext callContext) {
        final LogicalType arrayType =
                callContext.getArgumentDataTypes().get(ARRAY_POS).getLogicalType();
        if (!arrayType.is(LogicalTypeRoot.ARRAY)) {
            return Optional.empty();
        }
        final LogicalType arrayElementType = ((ArrayType) arrayType).getElementType();
        final LogicalType elementType =
                callContext.getArgumentDataTypes().get(ELEMENT_POS).getLogicalType();
        return LogicalTypeMerging.findCommonType(Arrays.asList(arrayElementType, elementType));
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
        if (argumentPos == ARRAY_POS) {
            return Argument.ofGroup(LogicalTypeRoot.ARRAY);
        }
        return Argument.of("<ARRAY ELEMENT>");
    }
}
