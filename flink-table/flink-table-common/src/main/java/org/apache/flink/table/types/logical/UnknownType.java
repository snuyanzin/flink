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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;

/** Unknown ype. */
@PublicEvolving
public class UnknownType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "UNKNOWN";

    private static final Class<?> INPUT_CONVERSION = Object.class;

    private static final Class<?> DEFAULT_CONVERSION = Object.class;

    public UnknownType() {
        super(false, LogicalTypeRoot.NULL);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new UnknownType();
    }

    @Override
    public String asSerializableString() {
        return FORMAT;
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_CONVERSION.equals(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        // any nullable class is supported
        return !clazz.isPrimitive();
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
