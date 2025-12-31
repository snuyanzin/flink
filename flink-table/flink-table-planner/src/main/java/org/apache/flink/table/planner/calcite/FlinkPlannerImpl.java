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

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.ddl.SqlCompilePlan;
import org.apache.flink.sql.parser.ddl.SqlReset;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseModules;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlBeginStatementSet;
import org.apache.flink.sql.parser.dml.SqlCompileAndExecutePlan;
import org.apache.flink.sql.parser.dml.SqlEndStatementSet;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlExecutePlan;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.sql.parser.dml.SqlTruncateTable;
import org.apache.flink.sql.parser.dql.SqlDescribeJob;
import org.apache.flink.sql.parser.dql.SqlLoadModule;
import org.apache.flink.sql.parser.dql.SqlRichDescribeFunction;
import org.apache.flink.sql.parser.dql.SqlRichDescribeModel;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;
import org.apache.flink.sql.parser.dql.SqlShowColumns;
import org.apache.flink.sql.parser.dql.SqlShowCurrentCatalog;
import org.apache.flink.sql.parser.dql.SqlShowCurrentDatabase;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.sql.parser.dql.SqlShowJars;
import org.apache.flink.sql.parser.dql.SqlShowJobs;
import org.apache.flink.sql.parser.dql.SqlShowModels;
import org.apache.flink.sql.parser.dql.SqlShowModules;
import org.apache.flink.sql.parser.dql.SqlShowPartitions;
import org.apache.flink.sql.parser.dql.SqlShowProcedures;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.dql.SqlUnloadModule;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl. We need it in order to share the planner
 * between the Table API relational plans and the SQL relation plans that are created by the Calcite
 * parser. The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
public class FlinkPlannerImpl {
    /**
     * the null default direction if not specified. Consistent with HIVE/SPARK/MYSQL/FLINK-RUNTIME.
     * So the default value only is set [[NullCollation.LOW]] for keeping consistent with
     * FLINK-RUNTIME. [[NullCollation.LOW]] means null values appear first when the order is ASC
     * (ascending), and ordered last when the order is DESC (descending).
     */
    public static final NullCollation DEFAULT_NULL_COLLATION = NullCollation.LOW;

    /** the default field collation if not specified, Consistent with CALCITE. */
    public static final Direction DEFAULT_COLLATION_DIRECTION = Direction.ASCENDING;

    public final FrameworkConfig config;
    protected final Function<Boolean, CalciteCatalogReader> catalogReaderSupplier;
    protected final FlinkTypeFactory typeFactory;
    protected final RelOptCluster cluster;
    public final SqlOperatorTable operatorTable;
    protected final CalciteParser parser;
    protected final SqlRexConvertletTable convertletTable;
    protected final SqlToRelConverter.Config sqlToRelConverterConfig;
    protected FlinkCalciteSqlValidator validator = null;

    public FlinkPlannerImpl(
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

    protected SqlNode validate(SqlNode sqlNode, FlinkCalciteSqlValidator validator) {
        try {
            sqlNode.accept(new PreValidateReWriter(validator, typeFactory));
            // do extended validation.
            if (sqlNode instanceof ExtendedSqlNode) {
                ((ExtendedSqlNode) sqlNode).validate();
            }
            // no need to validate row type for DDL and insert nodes.
            if (needToValidate(sqlNode)) {
                return sqlNode;
            }
            if (sqlNode instanceof SqlRichExplain) {
                final SqlRichExplain richExplain = (SqlRichExplain) sqlNode;
                final SqlNode validatedStatement = richExplain.getStatement();
                if (validatedStatement instanceof RichSqlInsert) {
                    // only validate source here
                    validateRichSqlInsert((RichSqlInsert) validatedStatement);
                } else {
                    validate(validatedStatement);
                }
                richExplain.setOperand(0, validatedStatement);
                return richExplain;
            } else if (sqlNode instanceof SqlStatementSet) {
                final SqlStatementSet statementSet = (SqlStatementSet) sqlNode;
                List<RichSqlInsert> inserts = statementSet.getInserts();
                for (int i = 0; i < inserts.size(); i++) {
                    statementSet.setOperand(i, validate(inserts.get(i)));
                }
                return statementSet;
            } else if (sqlNode instanceof SqlExecute) {
                SqlExecute execute = (SqlExecute) sqlNode;
                execute.setOperand(0, validate(execute.getStatement()));
                return execute;
            } else if (sqlNode instanceof RichSqlInsert) {
                return validateRichSqlInsert((RichSqlInsert) sqlNode);
            } else if (sqlNode instanceof SqlCompilePlan) {
                final SqlCompilePlan compile = (SqlCompilePlan) sqlNode;
                compile.setOperand(0, validate(compile.getOperandList().get(0)));
                return compile;
            } else if (sqlNode instanceof SqlCompileAndExecutePlan) {
                final SqlCompileAndExecutePlan compileAndExecutePlan =
                        (SqlCompileAndExecutePlan) sqlNode;
                compileAndExecutePlan.setOperand(
                        0, validate(compileAndExecutePlan.getOperandList().get(0)));
                return compileAndExecutePlan;
                // for call procedure statement
            } else if (sqlNode.getKind() == SqlKind.PROCEDURE_CALL) {
                final SqlBasicCall sqlCall = (SqlBasicCall) sqlNode;
                final List<SqlNode> operands = sqlCall.getOperandList();
                for (int i = 0; i < operands.size(); i++) {
                    sqlCall.setOperand(i, validate(operands.get(i)));
                }
                return sqlCall;
            } else {
                return validator.validate(sqlNode);
            }
        } catch (RuntimeException | SqlValidateException e) {
            throw new ValidationException("SQL validation failed. " + e.getMessage(), e);
        }
    }

    private boolean needToValidate(SqlNode sqlNode) {
        return sqlNode.getKind().belongsTo(SqlKind.DDL)
                || sqlNode.getKind() == SqlKind.CREATE_FUNCTION
                || sqlNode.getKind() == SqlKind.DROP_FUNCTION
                || sqlNode.getKind() == SqlKind.OTHER_DDL
                || sqlNode instanceof SqlLoadModule
                || sqlNode instanceof SqlShowCatalogs
                || sqlNode instanceof SqlShowCurrentCatalog
                || sqlNode instanceof SqlShowDatabases
                || sqlNode instanceof SqlShowCurrentDatabase
                || sqlNode instanceof SqlShowTables
                || sqlNode instanceof SqlShowModels
                || sqlNode instanceof SqlShowFunctions
                || sqlNode instanceof SqlShowJars
                || sqlNode instanceof SqlShowModules
                || sqlNode instanceof SqlShowColumns
                || sqlNode instanceof SqlShowPartitions
                || sqlNode instanceof SqlShowProcedures
                || sqlNode instanceof SqlShowJobs
                || sqlNode instanceof SqlDescribeJob
                || sqlNode instanceof SqlRichDescribeFunction
                || sqlNode instanceof SqlRichDescribeModel
                || sqlNode instanceof SqlRichDescribeTable
                || sqlNode instanceof SqlUnloadModule
                || sqlNode instanceof SqlUseModules
                || sqlNode instanceof SqlBeginStatementSet
                || sqlNode instanceof SqlEndStatementSet
                || sqlNode instanceof SqlSet
                || sqlNode instanceof SqlReset
                || sqlNode instanceof SqlExecutePlan
                || sqlNode instanceof SqlTruncateTable;
    }

    public SqlNode validate(SqlNode sqlNode) {
        final FlinkCalciteSqlValidator validator = getOrCreateSqlValidator();
        return validate(sqlNode, validator);
    }

    public SqlAdvisorValidator getSqlAdvisorValidator() {
        return new SqlAdvisorValidator(
                operatorTable,
                catalogReaderSupplier.apply(true), // ignore cases for lenient completion
                typeFactory,
                SqlValidator.Config.DEFAULT.withConformance(
                        config.getParserConfig().conformance()));
    }

    public RelRoot rel(SqlNode validatedSqlNode) {
        return rel(validatedSqlNode, getOrCreateSqlValidator());
    }

    protected RelRoot rel(SqlNode validatedSqlNode, FlinkCalciteSqlValidator sqlValidator) {
        assert (validatedSqlNode != null);
        // check whether this SqlNode tree contains query hints
        final CheckContainQueryHintsShuttle checkContainQueryHintsShuttle =
                new CheckContainQueryHintsShuttle();
        validatedSqlNode.accept(checkContainQueryHintsShuttle);
        final SqlToRelConverter sqlToRelConverter;
        if (checkContainQueryHintsShuttle.containsQueryHints) {
            final SqlToRelConverter converter =
                    createSqlToRelConverter(
                            sqlValidator,
                            // disable project merge during sql to rel phase to prevent
                            // incorrect propagation of query hints into child query block
                            sqlToRelConverterConfig.addRelBuilderConfigTransform(
                                    c -> c.withBloat(-1)));
            // TODO currently, it is a relatively hacked way to tell converter
            // that this SqlNode tree contains query hints
            converter.containsQueryHints();
            sqlToRelConverter = converter;
        } else {
            sqlToRelConverter = createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig);
        }

        return sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
        // we disable automatic flattening in order to let composite types pass without modification
        // we might enable it again once Calcite has better support for structured types
        // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

        // TableEnvironment.optimize will execute the following
        // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
        // convert time indicators
        // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
    }

    /**
     * Get the [[FlinkCalciteSqlValidator]] instance from this planner, create a new instance if
     * current validator has not been initialized, or returns the validator instance directly.
     *
     * <p>The validator instance creation is not thread safe.
     *
     * @return a new validator instance or current existed one
     */
    public FlinkCalciteSqlValidator getOrCreateSqlValidator() {
        if (validator == null) {
            CalciteCatalogReader catalogReader = catalogReaderSupplier.apply(false);
            validator = createSqlValidator(catalogReader);
        }
        return validator;
    }

    protected FlinkCalciteSqlValidator createSqlValidator(CalciteCatalogReader catalogReader) {
        final FlinkCalciteSqlValidator validator =
                new FlinkCalciteSqlValidator(
                        operatorTable,
                        catalogReader,
                        typeFactory,
                        SqlValidator.Config.DEFAULT
                                .withIdentifierExpansion(true)
                                .withDefaultNullCollation(FlinkPlannerImpl.DEFAULT_NULL_COLLATION)
                                .withTypeCoercionEnabled(false)
                                .withConformance(FlinkSqlConformance.DEFAULT),
                        createToRelContext(),
                        cluster,
                        config); // Disable implicit type coercion for now.
        return validator;
    }

    protected SqlToRelConverter createSqlToRelConverter(
            SqlValidator sqlValidator, SqlToRelConverter.Config config) {
        return new SqlToRelConverter(
                createToRelContext(),
                sqlValidator,
                sqlValidator.getCatalogReader().unwrap(CalciteCatalogReader.class),
                cluster,
                convertletTable,
                config);
    }

    public SqlNode validateExpression(
            SqlNode sqlNode, RelDataType inputRowType, @Nullable RelDataType outputType) {
        return validateExpression(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType);
    }

    private SqlNode validateExpression(
            SqlNode sqlNode,
            FlinkCalciteSqlValidator sqlValidator,
            RelDataType inputRowType,
            @Nullable RelDataType outputType) {
        Map<String, RelDataType> nameToTypeMap =
                inputRowType.getFieldList().stream()
                        .collect(
                                Collectors.toMap(
                                        RelDataTypeField::getName,
                                        RelDataTypeField::getType,
                                        (type1, type2) -> type2));
        if (outputType != null) {
            sqlValidator.setExpectedOutputType(sqlNode, outputType);
        }
        return sqlValidator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    }

    public RexNode rex(
            SqlNode sqlNode, RelDataType inputRowType, @Nullable RelDataType outputType) {
        return rex(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType);
    }

    private RexNode rex(
            SqlNode sqlNode,
            FlinkCalciteSqlValidator sqlValidator,
            RelDataType inputRowType,
            @Nullable RelDataType outputType) {
        final SqlNode validatedSqlNode =
                validateExpression(sqlNode, sqlValidator, inputRowType, outputType);
        final SqlToRelConverter sqlToRelConverter =
                createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig);
        Map<String, RexNode> nameToNodeMap =
                inputRowType.getFieldList().stream()
                        .collect(
                                Collectors.toMap(
                                        RelDataTypeField::getName,
                                        f -> RexInputRef.of(f.getIndex(), inputRowType)));

        return sqlToRelConverter.convertExpression(validatedSqlNode, nameToNodeMap);
    }

    protected SqlNode validateRichSqlInsert(RichSqlInsert insert) {
        // We don't support UPSERT INTO semantics (see FLINK-24225).
        if (insert.isUpsert()) {
            throw new ValidationException(
                    "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
        }
        // only validate source here.
        // ignore row type which will be verified in table environment.
        final SqlNode validatedSource = validate(insert.getSource());
        insert.setOperand(2, validatedSource);
        return insert;
    }

    /** Creates a new instance of [[RelOptTable.ToRelContext]] for [[RelOptTable]]. */
    public ToRelContextImpl createToRelContext() {
        return new ToRelContextImpl();
    }

    /**
     * Implements [[RelOptTable.ToRelContext]] interface for [[RelOptTable]] and
     * [[org.apache.calcite.tools.Planner]].
     */
    public class ToRelContextImpl implements RelOptTable.ToRelContext {

        public RelOptCluster getCluster() {
            return cluster;
        }

        public List<RelHint> getTableHints() {
            return ImmutableList.of();
        }

        @Override
        public RelRoot expandView(
                RelDataType rowType,
                String queryString,
                List<String> schemaPath,
                @Nullable List<String> viewPath) {
            final SqlNode parsed = parser.parse(queryString);
            final CalciteCatalogReader originalReader = catalogReaderSupplier.apply(false);
            final FlinkCalciteCatalogReader readerWithPathAdjusted =
                    new FlinkCalciteCatalogReader(
                            originalReader.getRootSchema(),
                            List.of(schemaPath, List.of(schemaPath.get(0))),
                            originalReader.getTypeFactory(),
                            originalReader.getConfig());
            final FlinkCalciteSqlValidator validator = createSqlValidator(readerWithPathAdjusted);
            final SqlNode validated = validate(parsed, validator);
            return rel(validated, validator);
        }
    }

    protected class CheckContainQueryHintsShuttle extends SqlShuttle {
        boolean containsQueryHints = false;

        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                if (select.hasHints() && hasQueryHints(select.getHints().getList())) {
                    containsQueryHints = true;
                    return call;
                }
            } else if (call instanceof SqlTableRef) {
                SqlTableRef table = (SqlTableRef) call;
                final SqlNodeList hintList = (SqlNodeList) table.getOperandList().get(1);
                if (hasQueryHints(hintList.getList())) {
                    containsQueryHints = true;
                    return call;
                }
            }
            return super.visit(call);
        }

        private boolean hasQueryHints(List<SqlNode> hints) {
            for (SqlNode hint : hints) {
                if (hint instanceof SqlHint) {
                    SqlHint sqlHint = (SqlHint) hint;
                    final String hintName = sqlHint.getName();
                    if (FlinkHints.isQueryHint(hintName.toUpperCase(Locale.ROOT))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
