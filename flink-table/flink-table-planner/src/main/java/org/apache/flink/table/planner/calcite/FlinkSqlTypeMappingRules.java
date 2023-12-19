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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.sql.type.SqlTypeAssignmentRule;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeMappingRules;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** FlinkSqlTypeMappingRules for Flink. */
public class FlinkSqlTypeMappingRules extends SqlTypeMappingRules {

    public static FlinkSqlTypeMappingRules.Builder flinkBuilder() {
        return new FlinkSqlTypeMappingRules.Builder();
    }

    public static SqlTypeCoercionRule getSqlTypeCoercionRule() {
        Builder builder = FlinkSqlTypeMappingRules.flinkBuilder();
        builder.addAll(SqlTypeAssignmentRule.instance().getTypeMapping());
        final Set<SqlTypeName> rule = new HashSet<>();

        // Make numbers symmetrical,
        // and make VARCHAR, CHAR, BOOLEAN and TIMESTAMP castable to/from numbers
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.DOUBLE);

        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        rule.add(SqlTypeName.BOOLEAN);
        builder.add(SqlTypeName.BOOLEAN, rule);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        builder.add(SqlTypeName.TINYINT, rule);
        builder.add(SqlTypeName.SMALLINT, rule);
        builder.add(SqlTypeName.INTEGER, rule);
        builder.add(SqlTypeName.BIGINT, rule);
        builder.add(SqlTypeName.FLOAT, rule);
        builder.add(SqlTypeName.REAL, rule);
        builder.add(SqlTypeName.DECIMAL, rule);
        builder.add(SqlTypeName.DOUBLE, rule);
        builder.add(SqlTypeName.CHAR, rule);
        builder.add(SqlTypeName.VARCHAR, rule);

        // VARCHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
        // intervals
        builder.add(
                SqlTypeName.VARCHAR,
                builder.copyValues(SqlTypeName.VARCHAR)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.BOOLEAN)
                        .add(SqlTypeName.DATE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .addAll(SqlTypeName.BINARY_TYPES)
                        .addAll(SqlTypeName.NUMERIC_TYPES)
                        .addAll(SqlTypeName.INTERVAL_TYPES)
                        .build());

        // CHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
        // intervals
        builder.add(
                SqlTypeName.CHAR,
                builder.copyValues(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .add(SqlTypeName.BOOLEAN)
                        .add(SqlTypeName.DATE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .addAll(SqlTypeName.BINARY_TYPES)
                        .addAll(SqlTypeName.NUMERIC_TYPES)
                        .addAll(SqlTypeName.INTERVAL_TYPES)
                        .build());

        // BINARY is castable from VARBINARY, CHARACTERS.
        builder.add(
                SqlTypeName.BINARY,
                builder.copyValues(SqlTypeName.BINARY)
                        .add(SqlTypeName.VARBINARY)
                        .addAll(SqlTypeName.CHAR_TYPES)
                        .build());

        // VARBINARY is castable from BINARY, CHARACTERS.
        builder.add(
                SqlTypeName.VARBINARY,
                builder.copyValues(SqlTypeName.VARBINARY)
                        .add(SqlTypeName.BINARY)
                        .addAll(SqlTypeName.CHAR_TYPES)
                        .build());

        // Exact numeric types are castable from intervals
        for (SqlTypeName exactType : SqlTypeName.EXACT_TYPES) {
            builder.add(
                    exactType,
                    builder.copyValues(exactType).addAll(SqlTypeName.INTERVAL_TYPES).build());
        }

        // Intervals are castable from exact numeric
        for (SqlTypeName typeName : SqlTypeName.INTERVAL_TYPES) {
            builder.add(
                    typeName,
                    builder.copyValues(typeName)
                            .add(SqlTypeName.TINYINT)
                            .add(SqlTypeName.SMALLINT)
                            .add(SqlTypeName.INTEGER)
                            .add(SqlTypeName.BIGINT)
                            .add(SqlTypeName.DECIMAL)
                            .add(SqlTypeName.CHAR)
                            .add(SqlTypeName.VARCHAR)
                            .build());
        }

        // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
        builder.add(
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                builder.copyValues(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.DATE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                        .build());

        // DATE is castable from...
        builder.add(
                SqlTypeName.DATE,
                builder.copyValues(SqlTypeName.DATE)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .build());

        // TIME is castable from...
        builder.add(
                SqlTypeName.TIME,
                builder.copyValues(SqlTypeName.TIME)
                        .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .build());

        // TIME WITH LOCAL TIME ZONE is castable from...
        builder.add(
                SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
                builder.copyValues(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .build());

        // TIMESTAMP is castable from...
        builder.add(
                SqlTypeName.TIMESTAMP,
                builder.copyValues(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.DATE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .addAll(SqlTypeName.NUMERIC_TYPES)
                        .build());

        // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
        builder.add(
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                builder.copyValues(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.TIMESTAMP)
                        .add(SqlTypeName.DATE)
                        .add(SqlTypeName.TIME)
                        .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .addAll(SqlTypeName.BINARY_TYPES)
                        .addAll(SqlTypeName.NUMERIC_TYPES)
                        .build());
        return SqlTypeCoercionRule.instance(builder.map);
    }

    /** Builder for FlinkSqlTypeMappingRules. */
    public static class Builder {
        final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;
        final LoadingCache<Set<SqlTypeName>, ImmutableSet<SqlTypeName>> sets;

        /** Creates an empty {@link SqlTypeMappingRules.Builder}. */
        Builder() {
            this.map = new HashMap<>();
            this.sets =
                    CacheBuilder.newBuilder()
                            .build(CacheLoader.from(set -> Sets.immutableEnumSet(set)));
        }

        /** Add a map entry to the existing {@link SqlTypeMappingRules.Builder} mapping. */
        void add(SqlTypeName fromType, Set<SqlTypeName> toTypes) {
            try {
                map.put(fromType, sets.get(toTypes));
            } catch (UncheckedExecutionException | ExecutionException e) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e));
            }
        }

        /** Put all the type mappings to the {@link SqlTypeMappingRules.Builder}. */
        void addAll(Map<SqlTypeName, ImmutableSet<SqlTypeName>> typeMapping) {
            try {
                map.putAll(typeMapping);
            } catch (UncheckedExecutionException e) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e));
            }
        }

        /**
         * Copy the map values from key {@code typeName} and returns as a {@link
         * ImmutableSet.Builder}.
         */
        ImmutableSet.Builder<SqlTypeName> copyValues(SqlTypeName typeName) {
            return ImmutableSet.<SqlTypeName>builder().addAll(castNonNull(map.get(typeName)));
        }
    }
}
