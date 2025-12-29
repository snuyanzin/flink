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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator.ExplicitTableSqlSelect;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Utils around sql rewrite. */
public class SqlRewriterUtils {

    private SqlRewriterUtils() {}

    // This code snippet is copied from the SqlValidatorImpl.
    public static SqlNode maybeCast(
            SqlNode node,
            RelDataType currentType,
            RelDataType desiredType,
            RelDataTypeFactory typeFactory) {
        return SqlTypeUtil.equalSansNullability(typeFactory, currentType, desiredType)
                ? node
                : SqlStdOperatorTable.CAST.createCall(
                        SqlParserPos.ZERO, node, SqlTypeUtil.convertTypeToSpec(desiredType));
    }

    public static SqlCall rewriteSqlCall(
            SqlValidator validator,
            SqlCall call,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions,
            Supplier<String> unsupportedErrorMessage) {
        switch (call.getKind()) {
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) call;
                return rewriteSqlSelect(
                        validator, sqlSelect, targetRowType, assignedFields, targetPositions, true);
            case EXPLICIT_TABLE:
                final List<SqlNode> tableOperands = call.getOperandList();
                final ExplicitTableSqlSelect expTable =
                        new ExplicitTableSqlSelect((SqlIdentifier) tableOperands.get(0), List.of());
                return rewriteSqlSelect(
                        validator, expTable, targetRowType, assignedFields, targetPositions, true);
            case VALUES:
                return validateAndRewriteSqlValues(
                        validator, call, targetRowType, assignedFields, targetPositions);
            case EXCEPT:
            case INTERSECT:
            case UNION:
                return rewriteSqlSetOp(
                        validator,
                        call,
                        targetRowType,
                        assignedFields,
                        targetPositions,
                        unsupportedErrorMessage);
            case ORDER_BY:
                return rewriteSqlOrderBy(
                        validator,
                        call,
                        targetRowType,
                        assignedFields,
                        targetPositions,
                        unsupportedErrorMessage);
            case WITH:
                return rewriteSqlWith(
                        validator, (SqlWith) call, targetRowType, assignedFields, targetPositions);
            default:
                throw new ValidationException(unsupportedErrorMessage.get());
        }
    }

    public static SqlCall rewriteSqlWith(
            SqlValidator validator,
            SqlWith cte,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions) {
        // Expands the select list first in case there is a star(*).
        // Validates the select first to register the where scope.
        validator.validate(cte);
        List<SqlSelect> selects = new ArrayList<>();
        extractSelectsFromCte((SqlCall) cte.body, selects);

        for (SqlSelect select : selects) {
            rewriteSqlSelect(
                    validator, select, targetRowType, assignedFields, targetPositions, false);
        }
        return cte;
    }

    private static void extractSelectsFromCte(SqlCall cte, List<SqlSelect> selects) {
        if (cte instanceof SqlSelect) {
            selects.add((SqlSelect) cte);
            return;
        }

        for (SqlNode sqlNode : cte.getOperandList()) {
            if (sqlNode instanceof SqlCall) {
                extractSelectsFromCte((SqlCall) sqlNode, selects);
            }
        }
    }

    private static CalciteContextException newValidationError(
            SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        assert (node != null);
        SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }

    private static List<SqlNode> getReorderedNodes(
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions,
            List<SqlNode> valueAsList) {
        final List<SqlNode> currentNodes;
        if (targetPositions.isEmpty()) {
            currentNodes = new ArrayList<>(valueAsList);
        } else {
            currentNodes = reorder(valueAsList, targetPositions);
        }

        List<SqlNode> fieldNodes = new ArrayList<>();
        for (int i = 0; i < targetRowType.getFieldCount(); i++) {
            if (assignedFields.containsKey(i)) {
                fieldNodes.add(assignedFields.get(i));
            } else if (!currentNodes.isEmpty()) {
                fieldNodes.add(currentNodes.remove(0));
            }
        }
        // Although it is error case, we still append the old remaining
        // value items to new item list.
        if (!currentNodes.isEmpty()) {
            fieldNodes.addAll(currentNodes);
        }
        return fieldNodes;
    }

    /**
     * Reorder sourceList to targetPosition. For example: - sourceList(f0, f1, f2). -
     * targetPosition(1, 2, 0). - Output(f1, f2, f0).
     *
     * @param sourceList input fields.
     * @param targetPositions reorder mapping.
     * @return reorder fields.
     */
    private static List<SqlNode> reorder(List<SqlNode> sourceList, List<Integer> targetPositions) {
        return targetPositions.stream().map(sourceList::get).collect(Collectors.toList());
    }

    private static SqlCall validateAndRewriteSqlValues(
            SqlValidator validator,
            SqlCall sValues,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions) {
        final List<SqlNode> valuesOperandList = sValues.getOperandList();
        for (SqlNode sqlNode : valuesOperandList) {
            if (sqlNode instanceof SqlCall) {
                if (!targetPositions.isEmpty()
                        && ((SqlCall) sqlNode).getOperandList().size() != targetPositions.size()) {
                    throw newValidationError(sValues, RESOURCE.columnCountMismatch());
                }
            }
        }
        validator.validate(sValues);
        return rewriteSqlValues(sValues, targetRowType, assignedFields, targetPositions);
    }

    private static SqlCall rewriteSqlValues(
            SqlCall values,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPosition) {
        List<SqlNode> fixedNodes = new ArrayList<>();
        List<SqlNode> operandList = values.getOperandList();
        for (final SqlNode value : operandList) {
            final List<SqlNode> valueAsList =
                    value.getKind() == SqlKind.ROW
                            ? ((SqlCall) value).getOperandList()
                            : List.of(value);
            final List<SqlNode> nodes =
                    getReorderedNodes(targetRowType, assignedFields, targetPosition, valueAsList);
            fixedNodes.add(SqlStdOperatorTable.ROW.createCall(value.getParserPosition(), nodes));
        }
        return SqlStdOperatorTable.VALUES.createCall(values.getParserPosition(), fixedNodes);
    }

    private static SqlCall rewriteSqlSelect(
            SqlValidator validator,
            SqlSelect select,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions,
            boolean validate) {
        // Expands the select list first in case there is a star(*).
        // Validates the select first to register the where scope.
        if (validate) {
            // In case of CTE there is just one validate call before multiple calls here
            validator.validate(select);
        }
        List<SqlNode> sourceList =
                validator.expandStar(select.getSelectList(), select, false).getList();

        if (!targetPositions.isEmpty() && sourceList.size() != targetPositions.size()) {
            throw newValidationError(select, RESOURCE.columnCountMismatch());
        }

        List<SqlNode> nodes =
                getReorderedNodes(targetRowType, assignedFields, targetPositions, sourceList);
        select.setSelectList(new SqlNodeList(nodes, select.getSelectList().getParserPosition()));
        return select;
    }

    private static SqlCall rewriteSqlSetOp(
            SqlValidator validator,
            SqlCall sqlSetOp,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions,
            Supplier<String> unsupportedErrorMessage) {
        // No need for validation since it will be called further in rewriteSqlCall
        final List<SqlNode> operandList = sqlSetOp.getOperandList();
        for (int i = 0; i < operandList.size(); i++) {
            SqlNode node = operandList.get(i);
            checkArgument(node instanceof SqlCall, node);
            sqlSetOp.setOperand(
                    i,
                    rewriteSqlCall(
                            validator,
                            (SqlCall) node,
                            targetRowType,
                            assignedFields,
                            targetPositions,
                            unsupportedErrorMessage));
        }
        return sqlSetOp;
    }

    private static SqlCall rewriteSqlOrderBy(
            SqlValidator validator,
            SqlCall sqlOrderBy,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPositions,
            Supplier<String> unsupportedErrorMessage) {
        final List<SqlNode> orderByOperands = sqlOrderBy.getOperandList();
        return new SqlOrderBy(
                sqlOrderBy.getParserPosition(),
                rewriteSqlCall(
                        validator,
                        (SqlCall) orderByOperands.get(0),
                        targetRowType,
                        assignedFields,
                        targetPositions,
                        unsupportedErrorMessage),
                (SqlNodeList) orderByOperands.get(1),
                orderByOperands.get(2),
                orderByOperands.get(3));
    }
}
