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

import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyCatalogSourceTable;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Static.RESOURCE;

/** Test. */
public class PreValidateReWriter extends SqlBasicVisitor<Void> {
    private final SqlValidator validator;
    private final RelDataTypeFactory typeFactory;

    public PreValidateReWriter(SqlValidator validator, RelDataTypeFactory typeFactory) {
        this.validator = validator;
        this.typeFactory = typeFactory;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlRichExplain) {
            final SqlNode statement = ((SqlRichExplain) call).getStatement();
            if (statement instanceof RichSqlInsert) {
                rewriteInsert((RichSqlInsert) statement);
            }
            // do nothing
        } else if (call instanceof RichSqlInsert) {
            rewriteInsert((RichSqlInsert) call);
        }
        // do nothing
        return null;
    }

    private void rewriteInsert(RichSqlInsert r) {
        if (r.getStaticPartitions().isEmpty() || r.getTargetColumnList() != null) {
            final SqlNode source = r.getSource();
            if (source instanceof SqlCall) {
                final SqlNode newSource =
                        appendPartitionAndNullsProjects(
                                r,
                                validator,
                                typeFactory,
                                (SqlCall) source,
                                r.getStaticPartitions());
                r.setOperand(2, newSource);
            } else {
                throw new ValidationException(notSupported(source));
            }
        }
    }

    /**
     * Append the static partitions and unspecified columns to the data source projection list. The
     * columns are appended to the corresponding positions.
     *
     * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt) whose partition columns
     * are (&lt;a&gt;, &lt;c&gt;), and got a query
     *
     * <blockquote>
     *
     * <pre> insert into A partition(a='11',
     * c='22') select b from B </pre>
     *
     * </blockquote>
     *
     * <p>The query would be rewritten to:
     *
     * <blockquote>
     *
     * <pre>
     * insert into A partition(a='11', c='22') select cast('11' as tpe1), b, cast('22' as tpe2) from B
     * </pre>
     *
     * </blockquote>
     *
     * <p>Where the "tpe1" and "tpe2" are data types of column a and c of target table A.
     *
     * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt), and got a query
     *
     * <blockquote>
     *
     * <pre> insert into A (a, b) select a, b from B </pre>
     *
     * </blockquote>
     *
     * <p>The query would be rewritten to:
     *
     * <blockquote>
     *
     * <pre> insert into A select a, b, cast(null as tpeC) from B
     * </pre>
     *
     * </blockquote>
     *
     * <p>Where the "tpeC" is data type of column c for target table A.
     *
     * @param sqlInsert RichSqlInsert instance
     * @param validator Validator
     * @param typeFactory type factory
     * @param source Source to rewrite
     * @param partitions Static partition statements
     */
    public SqlCall appendPartitionAndNullsProjects(
            RichSqlInsert sqlInsert,
            SqlValidator validator,
            RelDataTypeFactory typeFactory,
            SqlCall source,
            SqlNodeList partitions) {
        final CalciteCatalogReader calciteCatalogReader =
                validator.getCatalogReader().unwrap(CalciteCatalogReader.class);
        final SqlNode targetTable = sqlInsert.getTargetTable();
        final SqlIdentifier identifier;
        if (targetTable instanceof SqlIdentifier) {
            identifier = (SqlIdentifier) targetTable;
        } else if (targetTable instanceof SqlTableRef) {
            identifier = (SqlIdentifier) ((SqlTableRef) targetTable).getOperandList().get(0);
        } else {
            throw new ValidationException("Unexpected target table " + targetTable);
        }
        final List<String> names = identifier.names;

        final PreparingTable relOptTable = calciteCatalogReader.getTable(names);
        if (relOptTable == null) {
            // There is no table exists in current catalog,
            // just skip to let other validation error throw.
            return source;
        }

        final RelDataType targetRowType = createTargetRowType(typeFactory, relOptTable);
        // validate partition fields first.
        final LinkedHashMap<Integer, SqlNode> assignedFields = new LinkedHashMap<>();

        for (SqlNode node : partitions.getList()) {
            final SqlProperty sqlProperty = (SqlProperty) node;
            final SqlIdentifier id = sqlProperty.getKey();
            validateUnsupportedCompositeColumn(id);
            final RelDataTypeField targetField =
                    SqlValidatorUtil.getTargetField(
                            targetRowType, typeFactory, id, calciteCatalogReader, relOptTable);
            validateField(idx -> !assignedFields.containsKey(idx), id, targetField);
            final SqlLiteral value = (SqlLiteral) sqlProperty.getValue();
            assignedFields.put(
                    targetField.getIndex(),
                    SqlRewriterUtils.maybeCast(
                            value,
                            value.createSqlType(typeFactory),
                            targetField.getType(),
                            typeFactory));
        }

        // validate partial insert columns.

        // the columnList may reorder fields (compare with fields of sink)
        final List<Integer> targetPositions = new ArrayList<>();

        if (sqlInsert.getTargetColumnList() != null) {
            final Set<Integer> targetFields = new HashSet<>();
            List<RelDataTypeField> targetColumns =
                    sqlInsert.getTargetColumnList().getList().stream()
                            .map(
                                    id -> {
                                        final SqlIdentifier sqlIdentifier = (SqlIdentifier) id;
                                        validateUnsupportedCompositeColumn(sqlIdentifier);
                                        final RelDataTypeField targetField =
                                                SqlValidatorUtil.getTargetField(
                                                        targetRowType,
                                                        typeFactory,
                                                        sqlIdentifier,
                                                        calciteCatalogReader,
                                                        relOptTable);
                                        validateField(
                                                targetFields::add, (SqlIdentifier) id, targetField);
                                        return targetField;
                                    })
                            .collect(Collectors.toList());

            List<RelDataTypeField> partitionColumns =
                    partitions.getList().stream()
                            .map(
                                    property ->
                                            SqlValidatorUtil.getTargetField(
                                                    targetRowType,
                                                    typeFactory,
                                                    ((SqlProperty) property).getKey(),
                                                    calciteCatalogReader,
                                                    relOptTable))
                            .collect(Collectors.toList());

            for (RelDataTypeField targetField : targetRowType.getFieldList()) {
                if (!partitionColumns.contains(targetField)) {
                    if (!targetColumns.contains(targetField)) {
                        // padding null
                        SqlIdentifier id =
                                new SqlIdentifier(targetField.getName(), SqlParserPos.ZERO);
                        if (!targetField.getType().isNullable()) {
                            throw newValidationError(
                                    id, RESOURCE.columnNotNullable(targetField.getName()));
                        }
                        validateField(idx -> !assignedFields.containsKey(idx), id, targetField);
                        assignedFields.put(
                                targetField.getIndex(),
                                SqlRewriterUtils.maybeCast(
                                        SqlLiteral.createNull(SqlParserPos.ZERO),
                                        typeFactory.createUnknownType(),
                                        targetField.getType(),
                                        typeFactory));
                    } else {
                        // handle reorder
                        targetPositions.add(targetColumns.indexOf(targetField));
                    }
                }
            }
        }

        return SqlRewriterUtils.rewriteSqlCall(
                validator,
                source,
                targetRowType,
                assignedFields,
                targetPositions,
                () -> notSupported(source));
    }

    /**
     * Derives a physical row-type for INSERT and UPDATE operations.
     *
     * <p>This code snippet is almost inspired by
     * [[org.apache.calcite.sql.validate.SqlValidatorImpl#createTargetRowType]]. It is the best that
     * the logic can be merged into Apache Calcite, but this needs time.
     *
     * @param typeFactory TypeFactory
     * @param table Target table for INSERT/UPDATE
     * @return Rowtype
     */
    private RelDataType createTargetRowType(
            RelDataTypeFactory typeFactory, SqlValidatorTable table) {
        FlinkPreparingTableBase unwrappedTable = table.unwrap(FlinkPreparingTableBase.class);
        final TableSchema schema;
        if (unwrappedTable instanceof CatalogSourceTable) {
            schema = ((CatalogSourceTable) unwrappedTable).getCatalogTable().getSchema();
        } else if (unwrappedTable instanceof LegacyCatalogSourceTable) {
            schema = ((LegacyCatalogSourceTable) unwrappedTable).catalogTable().getSchema();
        } else {
            return table.getRowType();
        }
        return ((FlinkTypeFactory) typeFactory).buildPersistedRelNodeRowType(schema);
    }

    private void validateUnsupportedCompositeColumn(SqlIdentifier id) {
        assert (id != null);
        if (!id.isSimple()) {
            final SqlParserPos pos = id.getParserPosition();
            // TODO no suitable error message from current CalciteResource, just use this one
            // temporarily,
            // we will remove this after composite column name is supported.
            throw SqlUtil.newContextException(pos, RESOURCE.unknownTargetColumn(id.toString()));
        }
    }

    /** Check whether the field is valid. * */
    private void validateField(
            Function<Integer, Boolean> tester, SqlIdentifier id, RelDataTypeField targetField) {
        if (targetField == null) {
            throw newValidationError(id, RESOURCE.unknownTargetColumn(id.toString()));
        }
        if (!tester.apply(targetField.getIndex())) {
            throw newValidationError(id, RESOURCE.duplicateTargetColumn(targetField.getName()));
        }
    }

    private static String notSupported(SqlNode source) {
        return String.format(
                "INSERT INTO <table> PARTITION [(COLUMN LIST)] statement only support "
                        + "SELECT, VALUES, SET_QUERY AND ORDER BY clause for now, '%s' is not supported yet.",
                source);
    }

    private CalciteContextException newValidationError(
            SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        assert (node != null);
        final SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }
}
