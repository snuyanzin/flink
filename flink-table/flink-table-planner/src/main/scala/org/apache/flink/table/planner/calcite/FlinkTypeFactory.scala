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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.legacy.api.TableSchema
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType
import org.apache.flink.table.planner.calcite.FlinkTypeFactory2.toLogicalType
import org.apache.flink.table.planner.plan.schema._
import org.apache.flink.table.runtime.types.{LogicalTypeDataTypeConverter, PlannerTypeUtils}
import org.apache.flink.table.types.logical._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.utils.TableSchemaUtils

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.parser.SqlParserPos

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Flink specific type factory that represents the interface between Flink's [[LogicalType]] and
 * Calcite's [[RelDataType]].
 */
class FlinkTypeFactory(
    classLoader: ClassLoader,
    typeSystem: RelDataTypeSystem = FlinkTypeSystem.INSTANCE)
  extends FlinkTypeFactory2(typeSystem, classLoader) {

  /**
   * Create a calcite field type in table schema from [[LogicalType]]. It use PEEK_FIELDS_NO_EXPAND
   * when type is a nested struct type (Flink [[RowType]]).
   *
   * @param t
   *   flink logical type.
   * @return
   *   calcite [[RelDataType]].
   */
  def createFieldTypeFromLogicalType(t: LogicalType): RelDataType = {
    def newRelDataType(): RelDataType = t.getTypeRoot match {
      case LogicalTypeRoot.NULL => createSqlType(NULL)
      case LogicalTypeRoot.BOOLEAN => createSqlType(BOOLEAN)
      case LogicalTypeRoot.TINYINT => createSqlType(TINYINT)
      case LogicalTypeRoot.SMALLINT => createSqlType(SMALLINT)
      case LogicalTypeRoot.INTEGER => createSqlType(INTEGER)
      case LogicalTypeRoot.BIGINT => createSqlType(BIGINT)
      case LogicalTypeRoot.FLOAT => createSqlType(FLOAT)
      case LogicalTypeRoot.DOUBLE => createSqlType(DOUBLE)
      case LogicalTypeRoot.VARCHAR => createSqlType(VARCHAR, t.asInstanceOf[VarCharType].getLength)
      case LogicalTypeRoot.CHAR => createSqlType(CHAR, t.asInstanceOf[CharType].getLength)

      // temporal types
      case LogicalTypeRoot.DATE => createSqlType(DATE)
      case LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE => createSqlType(TIME)

      // interval types
      case LogicalTypeRoot.INTERVAL_YEAR_MONTH =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))
      case LogicalTypeRoot.INTERVAL_DAY_TIME =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

      case LogicalTypeRoot.BINARY => createSqlType(BINARY, t.asInstanceOf[BinaryType].getLength)
      case LogicalTypeRoot.VARBINARY =>
        createSqlType(VARBINARY, t.asInstanceOf[VarBinaryType].getLength)

      case LogicalTypeRoot.DECIMAL =>
        t match {
          case decimalType: DecimalType =>
            createSqlType(DECIMAL, decimalType.getPrecision, decimalType.getScale)
          case legacyType: LegacyTypeInformationType[_]
              if legacyType.getTypeInformation == BasicTypeInfo.BIG_DEC_TYPE_INFO =>
            createSqlType(DECIMAL, 38, 18)
        }

      case LogicalTypeRoot.ROW =>
        val rowType = t.asInstanceOf[RowType]
        buildStructType(
          rowType.getFieldNames,
          rowType.getChildren,
          // fields are not expanded in "SELECT *"
          StructKind.PEEK_FIELDS_NO_EXPAND)

      case LogicalTypeRoot.STRUCTURED_TYPE =>
        t match {
          case structuredType: StructuredType => StructuredRelDataType.create(this, structuredType)
          case legacyTypeInformationType: LegacyTypeInformationType[_] =>
            createFieldTypeFromLogicalType(
              PlannerTypeUtils.removeLegacyTypes(legacyTypeInformationType))
        }

      case LogicalTypeRoot.ARRAY =>
        val arrayType = t.asInstanceOf[ArrayType]
        createArrayType(createFieldTypeFromLogicalType(arrayType.getElementType), -1)

      case LogicalTypeRoot.MAP =>
        val mapType = t.asInstanceOf[MapType]
        createMapType(
          createFieldTypeFromLogicalType(mapType.getKeyType),
          createFieldTypeFromLogicalType(mapType.getValueType))

      case LogicalTypeRoot.MULTISET =>
        val multisetType = t.asInstanceOf[MultisetType]
        createMultisetType(createFieldTypeFromLogicalType(multisetType.getElementType), -1)

      case LogicalTypeRoot.RAW =>
        t match {
          case rawType: RawType[_] =>
            new RawRelDataType(rawType)
          case genericType: TypeInformationRawType[_] =>
            new GenericRelDataType(genericType, true, getTypeSystem)
          case legacyType: LegacyTypeInformationType[_] =>
            createFieldTypeFromLogicalType(PlannerTypeUtils.removeLegacyTypes(legacyType))
        }

      case LogicalTypeRoot.SYMBOL =>
        createSqlType(SqlTypeName.SYMBOL)

      case LogicalTypeRoot.DESCRIPTOR =>
        createSqlType(SqlTypeName.COLUMN_LIST)

      case LogicalTypeRoot.VARIANT =>
        createSqlType(SqlTypeName.VARIANT)

      case _ @t =>
        throw new TableException(s"Type is not supported: $t")
    }

    // Kind in TimestampType do not affect the hashcode and equals, So we can't put it to seenTypes
    val relType = t.getTypeRoot match {
      case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
        val timestampType = t.asInstanceOf[TimestampType]
        timestampType.getKind match {
          case TimestampKind.ROWTIME => createRowtimeIndicatorType(t.isNullable, false)
          case TimestampKind.REGULAR => createSqlType(TIMESTAMP, timestampType.getPrecision)
          case TimestampKind.PROCTIME =>
            throw new TableException(
              s"Processing time indicator only supports" +
                s" LocalZonedTimestampType, but actual is TimestampType." +
                s" This is a bug in planner, please file an issue.")
        }
      case LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val lzTs = t.asInstanceOf[LocalZonedTimestampType]
        lzTs.getKind match {
          case TimestampKind.PROCTIME => createProctimeIndicatorType(t.isNullable)
          case TimestampKind.ROWTIME => createRowtimeIndicatorType(t.isNullable, true)
          case TimestampKind.REGULAR =>
            createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, lzTs.getPrecision)
        }
      case _ =>
        if (seenTypes.contains(t)) {
          seenTypes(t)
        } else {
          val refType = newRelDataType()
          seenTypes.put(t, refType)
          refType
        }
    }

    createTypeWithNullability(relType, t.isNullable)
  }

  /** Creates a indicator type for processing-time, but with similar properties as SQL timestamp. */
  def createProctimeIndicatorType(isNullable: Boolean): RelDataType = {
    val originalType = createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3))
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isNullable,
        isEventTime = false))
  }

  /** Creates a indicator type for event-time, but with similar properties as SQL timestamp. */
  def createRowtimeIndicatorType(isNullable: Boolean, isTimestampLtz: Boolean): RelDataType = {
    val originalType = if (isTimestampLtz) {
      createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3))
    } else {
      createFieldTypeFromLogicalType(new TimestampType(isNullable, 3))
    }

    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isNullable,
        isEventTime = true))
  }

  /**
   * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
   *
   * @param tableSchema
   *   schema to convert to Calcite's specific one
   * @return
   *   a struct type with the input fieldNames, input fieldTypes, and system fields
   */
  def buildRelNodeRowType(tableSchema: TableSchema): RelDataType = {
    buildRelNodeRowType(
      tableSchema.getFieldNames,
      tableSchema.getFieldDataTypes.map(_.getLogicalType))
  }

  /**
   * Creates a table row type with the given field names and field types. Table row type is table
   * schema for Calcite [[RelNode]]. See [[RelNode#getRowType]].
   *
   * It uses [[StructKind#FULLY_QUALIFIED]] to let each field must be referenced explicitly.
   *
   * @param fieldNames
   *   field names
   * @param fieldTypes
   *   field types, every element is Flink's [[LogicalType]]
   * @return
   *   a table row type with the input fieldNames, input fieldTypes.
   */
  def buildRelNodeRowType(
      fieldNames: util.List[String],
      fieldTypes: util.List[LogicalType]): RelDataType = {
    buildStructType(fieldNames, fieldTypes, StructKind.FULLY_QUALIFIED)
  }

  /**
   * Creates a table row type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
   * Table row type is table schema for Calcite RelNode. See getRowType of [[RelNode]]. Use
   * FULLY_QUALIFIED to let each field must be referenced explicitly.
   *
   * @param fieldNames
   *   field names
   * @param fieldTypes
   *   field types, every element is Flink's [[LogicalType]]
   * @return
   *   a table row type with the input fieldNames, input fieldTypes.
   */
  def buildRelNodeRowType(fieldNames: Seq[String], fieldTypes: Seq[LogicalType]): RelDataType = {
    buildStructType(fieldNames, fieldTypes, StructKind.FULLY_QUALIFIED)
  }

  /**
   * Creates a table row type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
   * Table row type is table schema for Calcite RelNode. See getRowType of [[RelNode]]. Use
   * FULLY_QUALIFIED to let each field must be referenced explicitly.
   */
  def buildRelNodeRowType(rowType: RowType): RelDataType = {
    val fields = rowType.getFields
    buildStructType(fields.map(_.getName), fields.map(_.getType), StructKind.FULLY_QUALIFIED)
  }

  /**
   * Creates a struct type with the physical columns using FlinkTypeFactory
   *
   * @param tableSchema
   *   schema to convert to Calcite's specific one
   * @return
   *   a struct type with the input fieldNames, input fieldTypes.
   */
  def buildPhysicalRelNodeRowType(tableSchema: TableSchema): RelDataType = {
    buildRelNodeRowType(TableSchemaUtils.getPhysicalSchema(tableSchema))
  }

  /**
   * Creats a struct type with the persisted columns using FlinkTypeFactory
   *
   * @param tableSchema
   *   schema to convert to Calcite's specific one
   * @return
   *   a struct type with the input fieldsNames, input fieldTypes.
   */
  def buildPersistedRelNodeRowType(tableSchema: TableSchema): RelDataType = {
    buildRelNodeRowType(TableSchemaUtils.getPersistedSchema(tableSchema))
  }

  /**
   * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
   *
   * @param fieldNames
   *   field names
   * @param fieldTypes
   *   field types, every element is Flink's [[LogicalType]].
   * @param structKind
   *   Name resolution policy. See more information in [[StructKind]].
   * @return
   *   a struct type with the input fieldNames, input fieldTypes.
   */
  private def buildStructType(
      fieldNames: Seq[String],
      fieldTypes: Seq[LogicalType],
      structKind: StructKind): RelDataType = {
    val b = builder
    b.kind(structKind)
    val fields = fieldNames.zip(fieldTypes)
    fields.foreach {
      case (fieldName, fieldType) =>
        val fieldRelDataType = createFieldTypeFromLogicalType(fieldType)
        checkForNullType(fieldRelDataType)
        b.add(fieldName, fieldRelDataType)
    }
    b.build
  }

  /** Returns a projected [[RelDataType]] of the structure type. */
  def projectStructType(relType: RelDataType, selectedFields: Array[Int]): RelDataType = {
    this.createStructType(
      selectedFields
        .map(idx => relType.getFieldList.get(idx))
        .toList
        .asJava)
  }

  // ----------------------------------------------------------------------------------------------

  override def createRawType(className: String, serializerString: String): RelDataType = {
    val rawType = RawType.restore(classLoader, className, serializerString)
    val rawRelDataType = createFieldTypeFromLogicalType(rawType)
    canonize(rawRelDataType)
  }

}

object FlinkTypeFactory {

  def isTimeIndicatorType(t: LogicalType): Boolean = t match {
    case t: TimestampType if t.getKind == TimestampKind.ROWTIME => true
    case ltz: LocalZonedTimestampType if ltz.getKind == TimestampKind.PROCTIME => true
    case _ => false
  }

  def isTimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case _: TimeIndicatorRelDataType => true
    case _ => false
  }

  def isRowtimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if ti.isEventTime => true
    case _ => false
  }

  def isProctimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if !ti.isEventTime => true
    case _ => false
  }

  def isTimestampLtzIndicatorType(relDataType: RelDataType): Boolean =
    relDataType match {
      case ti: TimeIndicatorRelDataType
          if ti.originalType.getSqlTypeName.equals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
        true
      case _ => false
    }

  @Deprecated
  def isProctimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if !ti.isEventTime => true
    case _ => false
  }

  @Deprecated
  def isRowtimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if ti.isEventTime => true
    case _ => false
  }

  @Deprecated
  def isTimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo => true
    case _ => false
  }

  def toTableSchema(relDataType: RelDataType): TableSchema = {
    val fieldNames = relDataType.getFieldNames.toArray(new Array[String](0))
    val fieldTypes = relDataType.getFieldList.asScala
      .map(
        field =>
          LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(toLogicalType(field.getType)))
      .toArray
    TableSchema.builder.fields(fieldNames, fieldTypes).build
  }
}
