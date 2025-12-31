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
package org.apache.flink.table.planner.calcite

import org.apache.flink.sql.parser.ExtendedSqlNode
import org.apache.flink.sql.parser.ddl.{SqlCompilePlan, SqlReset, SqlSet, SqlUseModules}
import org.apache.flink.sql.parser.dml._
import org.apache.flink.sql.parser.dql._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.sql.{SqlBasicCall, SqlCall, SqlHint, SqlKind, SqlNode, SqlNodeList, SqlSelect, SqlTableRef}
import org.apache.calcite.sql.advise.SqlAdvisorValidator
import org.apache.calcite.sql.util.SqlShuttle
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}

import java.lang.{Boolean => JBoolean}
import java.util
import java.util.Locale
import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl. We need it in order to share the planner
 * between the Table API relational plans and the SQL relation plans that are created by the Calcite
 * parser. The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
class FlinkPlannerImpl(
    config: FrameworkConfig,
    catalogReaderSupplier: JFunction[JBoolean, CalciteCatalogReader],
    typeFactory: FlinkTypeFactory,
    cluster: RelOptCluster)
  extends FlinkPlannerImpl2(config, catalogReaderSupplier, typeFactory, cluster) {

  protected def validate(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator): SqlNode = {
    try {
      sqlNode.accept(new PreValidateReWriter(validator, typeFactory))
      // do extended validation.
      sqlNode match {
        case node: ExtendedSqlNode =>
          node.validate()
        case _ =>
      }
      // no need to validate row type for DDL and insert nodes.
      if (
        sqlNode.getKind.belongsTo(SqlKind.DDL)
        || sqlNode.getKind == SqlKind.CREATE_FUNCTION
        || sqlNode.getKind == SqlKind.DROP_FUNCTION
        || sqlNode.getKind == SqlKind.OTHER_DDL
        || sqlNode.isInstanceOf[SqlLoadModule]
        || sqlNode.isInstanceOf[SqlShowCatalogs]
        || sqlNode.isInstanceOf[SqlShowCurrentCatalog]
        || sqlNode.isInstanceOf[SqlShowDatabases]
        || sqlNode.isInstanceOf[SqlShowCurrentDatabase]
        || sqlNode.isInstanceOf[SqlShowTables]
        || sqlNode.isInstanceOf[SqlShowModels]
        || sqlNode.isInstanceOf[SqlShowFunctions]
        || sqlNode.isInstanceOf[SqlShowJars]
        || sqlNode.isInstanceOf[SqlShowModules]
        || sqlNode.isInstanceOf[SqlShowColumns]
        || sqlNode.isInstanceOf[SqlShowPartitions]
        || sqlNode.isInstanceOf[SqlShowProcedures]
        || sqlNode.isInstanceOf[SqlShowJobs]
        || sqlNode.isInstanceOf[SqlDescribeJob]
        || sqlNode.isInstanceOf[SqlRichDescribeFunction]
        || sqlNode.isInstanceOf[SqlRichDescribeModel]
        || sqlNode.isInstanceOf[SqlRichDescribeTable]
        || sqlNode.isInstanceOf[SqlUnloadModule]
        || sqlNode.isInstanceOf[SqlUseModules]
        || sqlNode.isInstanceOf[SqlBeginStatementSet]
        || sqlNode.isInstanceOf[SqlEndStatementSet]
        || sqlNode.isInstanceOf[SqlSet]
        || sqlNode.isInstanceOf[SqlReset]
        || sqlNode.isInstanceOf[SqlExecutePlan]
        || sqlNode.isInstanceOf[SqlTruncateTable]
      ) {
        return sqlNode
      }
      sqlNode match {
        case richExplain: SqlRichExplain =>
          val validatedStatement = richExplain.getStatement match {
            // only validate source here
            case insert: RichSqlInsert =>
              validateRichSqlInsert(insert)
            case others =>
              validate(others)
          }
          richExplain.setOperand(0, validatedStatement)
          richExplain
        case statementSet: SqlStatementSet =>
          statementSet.getInserts.asScala.zipWithIndex.foreach {
            case (insert, idx) => statementSet.setOperand(idx, validate(insert))
          }
          statementSet
        case execute: SqlExecute =>
          execute.setOperand(0, validate(execute.getStatement))
          execute
        case insert: RichSqlInsert =>
          validateRichSqlInsert(insert)
        case compile: SqlCompilePlan =>
          compile.setOperand(0, validate(compile.getOperandList.get(0)))
          compile
        case compileAndExecute: SqlCompileAndExecutePlan =>
          compileAndExecute.setOperand(0, validate(compileAndExecute.getOperandList.get(0)))
          compileAndExecute
        // for call procedure statement
        case sqlCallNode if sqlCallNode.getKind == SqlKind.PROCEDURE_CALL =>
          val callNode = sqlCallNode.asInstanceOf[SqlBasicCall]
          callNode.getOperandList.asScala.zipWithIndex.foreach {
            case (operand, idx) => callNode.setOperand(idx, validate(operand))
          }
          callNode
        case _ =>
          validator.validate(sqlNode)
      }
    } catch {
      case e: RuntimeException =>
        throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
    }
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    rel(validatedSqlNode, getOrCreateSqlValidator())
  }

  protected def rel(validatedSqlNode: SqlNode, sqlValidator: FlinkCalciteSqlValidator) = {
    try {
      assert(validatedSqlNode != null)
      // check whether this SqlNode tree contains query hints
      val checkContainQueryHintsShuttle = new CheckContainQueryHintsShuttle
      validatedSqlNode.accept(checkContainQueryHintsShuttle)
      val sqlToRelConverter: SqlToRelConverter =
        if (checkContainQueryHintsShuttle.containsQueryHints) {
          val converter = createSqlToRelConverter(
            sqlValidator,
            // disable project merge during sql to rel phase to prevent
            // incorrect propagation of query hints into child query block
            sqlToRelConverterConfig.addRelBuilderConfigTransform(c => c.withBloat(-1))
          )
          // TODO currently, it is a relatively hacked way to tell converter
          // that this SqlNode tree contains query hints
          converter.containsQueryHints()
          converter
        } else {
          createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig)
        }

      sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
      // we disable automatic flattening in order to let composite types pass without modification
      // we might enable it again once Calcite has better support for structured types
      // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

      // TableEnvironment.optimize will execute the following
      // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      // convert time indicators
      // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private class CheckContainQueryHintsShuttle extends SqlShuttle {
    var containsQueryHints: Boolean = false

    override def visit(call: SqlCall): SqlNode = {
      call match {
        case select: SqlSelect =>
          if (select.hasHints && hasQueryHints(select.getHints.getList)) {
            containsQueryHints = true
            return call
          }
        case table: SqlTableRef =>
          val hintList = table.getOperandList.get(1).asInstanceOf[SqlNodeList]
          if (hasQueryHints(hintList.getList)) {
            containsQueryHints = true
            return call
          }
        case _ => // ignore
      }
      super.visit(call)
    }

    private def hasQueryHints(hints: util.List[SqlNode]): Boolean = {
      JavaScalaConversionUtil.toScala(hints).foreach {
        case hint: SqlHint =>
          val hintName = hint.getName
          if (FlinkHints.isQueryHint(hintName.toUpperCase(Locale.ROOT))) {
            return true
          }
      }
      false
    }
  }
}
