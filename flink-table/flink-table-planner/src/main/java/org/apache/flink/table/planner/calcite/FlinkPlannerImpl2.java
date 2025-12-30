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

import org.apache.flink.table.planner.parse.CalciteParser;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;

import java.util.function.Function;

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl. We need it in order to share the planner
 * between the Table API relational plans and the SQL relation plans that are created by the Calcite
 * parser. The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
public class FlinkPlannerImpl2 {
    public final FrameworkConfig config;
    protected final Function<Boolean, CalciteCatalogReader> catalogReaderSupplier;
    protected final FlinkTypeFactory typeFactory;
    protected final RelOptCluster cluster;
    public final SqlOperatorTable operatorTable;
    protected final CalciteParser parser;
    protected final SqlRexConvertletTable convertletTable;
    protected final SqlToRelConverter.Config sqlToRelConverterConfig;

    public FlinkPlannerImpl2(
            FrameworkConfig config,
            Function<Boolean, CalciteCatalogReader> catalogReaderSupplier,
            FlinkTypeFactory typeFactory,
            RelOptCluster cluster) {
        this.catalogReaderSupplier = catalogReaderSupplier;
        this.config = config;
        this.typeFactory = typeFactory;
        this.cluster = cluster;
        this.operatorTable = config.getOperatorTable();
        this.parser = new CalciteParser(config.getParserConfig());
        this.convertletTable = config.getConvertletTable();
        this.sqlToRelConverterConfig =
                config.getSqlToRelConverterConfig().withAddJsonTypeOperatorEnabled(false);
    }

    public Function<Boolean, CalciteCatalogReader> getCatalogReaderSupplier() {
        return catalogReaderSupplier;
    }

    public RelOptCluster cluster() {
        return cluster;
    }

    public FrameworkConfig config() {
        return config;
    }

    public SqlRexConvertletTable getConvertletTable() {
        return convertletTable;
    }

    public SqlOperatorTable getOperatorTable() {
        return operatorTable;
    }

    public CalciteParser parser() {
        return parser;
    }

    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return sqlToRelConverterConfig;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }
}
