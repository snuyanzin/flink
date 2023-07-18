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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_FROM_ARRAYS}. */
@Internal
public class MapFromArraysFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter keyElementGetter;
    private final ArrayData.ElementGetter valueElementGetter;

    private final SpecializedFunction.ExpressionEvaluator keyEqualityEvaluator;
    private transient MethodHandle keyEqualityHandle;

    public MapFromArraysFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, context);
        KeyValueDataType outputType =
                ((KeyValueDataType) context.getCallContext().getOutputDataType().get());
        final DataType keyDataType = outputType.getKeyDataType();
        keyElementGetter =
                ArrayData.createElementGetter(outputType.getKeyDataType().getLogicalType());
        valueElementGetter =
                ArrayData.createElementGetter(outputType.getValueDataType().getLogicalType());

        keyEqualityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", keyDataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", keyDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        keyEqualityHandle = keyEqualityEvaluator.open(context);
    }

    public @Nullable MapData eval(@Nullable ArrayData keysArray, @Nullable ArrayData valuesArray) {
        if (keysArray == null || valuesArray == null) {
            return null;
        }

        if (keysArray.size() != valuesArray.size()) {
            throw new FlinkRuntimeException(
                    "Invalid function MAP_FROM_ARRAYS call:\n"
                            + "The length of the keys array "
                            + keysArray.size()
                            + " is not equal to the length of the values array "
                            + valuesArray.size());
        }

        try {
            return new MapDataForMapFromArrays(keysArray, valuesArray);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        keyEqualityEvaluator.close();
    }

    private class MapDataForMapFromArrays implements MapData {
        private final ArrayData keyArray;
        private final ArrayData valueArray;

        public MapDataForMapFromArrays(ArrayData keyArray, ArrayData valueArray) throws Throwable {
            List keys = new ArrayList();
            List values = new ArrayList();
            boolean nullKeyPresent = false;
            for (int i = keyArray.size() - 1; i >= 0; i--) {
                final Object key = keyElementGetter.getElementOrNull(keyArray, i);
                final Object value = valueElementGetter.getElementOrNull(valueArray, i);
                if (key == null) {
                    if (!nullKeyPresent) {
                        nullKeyPresent = true;
                        keys.add(key);
                        values.add(value);
                    }
                    continue;
                }
                boolean newKey = true;
                for (int j = 0; j < keys.size(); j++) {
                    final Object alreadyExistingKey = keys.get(j);
                    if (alreadyExistingKey != null
                            && (boolean) keyEqualityHandle.invoke(key, alreadyExistingKey)) {
                        newKey = false;
                        break;
                    }
                }
                if (newKey) {
                    keys.add(key);
                    values.add(value);
                }
            }
            this.keyArray = new GenericArrayData(keys.toArray());
            this.valueArray = new GenericArrayData(values.toArray());
        }

        @Override
        public int size() {
            return keyArray.size();
        }

        @Override
        public ArrayData keyArray() {
            return keyArray;
        }

        @Override
        public ArrayData valueArray() {
            return valueArray;
        }
    }
}
