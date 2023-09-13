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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{CloseableIterator, DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, Schema, Table, TableDescriptor, TableResult}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.planner.runtime.stream.sql.DataStreamScalaITCase.{ComplexCaseClass, ImmutableCaseClass}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertThat}

import java.util

import scala.collection.JavaConverters._

/** Tests for connecting to the Scala [[DataStream]] API. */
class DataStreamScalaITCase extends AbstractTestBase {

  private var env: StreamExecutionEnvironment = _

  private var tableEnv: StreamTableEnvironment = _

  @Before
  def before(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    tableEnv = StreamTableEnvironment.create(env)
  }

  @Test
  def testFromAndToDataStreamWithCaseClass(): Unit = {
    val caseClasses = Array(
      ComplexCaseClass(42, "hello", ImmutableCaseClass(42.0, b = true)),
      ComplexCaseClass(42, null, ImmutableCaseClass(42.0, b = false)))

    val dataStream = env.fromElements(caseClasses: _*)

    val table = tableEnv.fromDataStream(dataStream)

    testSchema(
      table,
      Column.physical("c", DataTypes.INT().notNull().bridgedTo(classOf[Int])),
      Column.physical("a", DataTypes.STRING()),
      Column.physical(
        "p",
        DataTypes
          .STRUCTURED(
            classOf[ImmutableCaseClass],
            DataTypes.FIELD("d", DataTypes.DOUBLE().notNull()), // serializer doesn't support null
            DataTypes.FIELD("b", DataTypes.BOOLEAN().notNull().bridgedTo(classOf[Boolean]))
          )
          .notNull()
      )
    )

    testResult(
      table.execute(),
      Row.of(Int.box(42), "hello", ImmutableCaseClass(42.0, b = true)),
      Row.of(Int.box(42), null, ImmutableCaseClass(42.0, b = false)))

    val resultStream = tableEnv.toDataStream(table, classOf[ComplexCaseClass])

    testResult(resultStream, caseClasses: _*)
  }

  @Test
  def testImplicitConversions(): Unit = {
    // DataStream to Table implicit
    val table = env.fromElements((42, "hello")).toTable(tableEnv)

    // Table to DataStream implicit
    assertEquals(List(Row.of(Int.box(42), "hello")), table.executeAndCollect().toList)
  }

  @Test
  def testIllegalArgumentForListAgg13(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val accountsTd = TableDescriptor
      .forConnector("datagen")
      .option("rows-per-second", "10")
      .option("number-of-rows", "10")
      .schema(
        Schema
          .newBuilder()
          .column("account_num", DataTypes.VARCHAR(2147483647))
          .column("acc_name", DataTypes.VARCHAR(2147483647))
          .column("acc_phone_num", DataTypes.VARCHAR(2147483647))
          .build())
      .build()

    val accountsTable = tableEnv.from(accountsTd)

    tableEnv.createTemporaryView("accounts", accountsTable)

    val transactionsTd = TableDescriptor
      .forConnector("datagen")
      .option("rows-per-second", "10")
      .option("number-of-rows", "10")
      .schema(
        Schema
          .newBuilder()
          .column("account_num", DataTypes.VARCHAR(2147483647))
          .column("transaction_place", DataTypes.VARCHAR(2147483647))
          .column("transaction_time", DataTypes.BIGINT())
          .column("amount", DataTypes.INT())
          .build())
      .build()

    val transactionsTable = tableEnv.from(transactionsTd)

    tableEnv.createTemporaryView("transaction_data", transactionsTable)

    val newTable = tableEnv.sqlQuery(
      "select   acc.account_num,  (select count(*) from transaction_data where transaction_place = trans.transaction_place and account_num = acc.account_num)  from  accounts acc,transaction_data trans")

    tableEnv.toChangelogStream(newTable).print()

    env.execute()
  }

  // --------------------------------------------------------------------------------------------
  // Helper methods
  // --------------------------------------------------------------------------------------------

  private def testSchema(table: Table, expectedColumns: Column*): Unit = {
    assertEquals(ResolvedSchema.of(expectedColumns: _*), table.getResolvedSchema)
  }

  private def testResult(result: TableResult, expectedRows: Row*): Unit = {
    val actualRows: util.List[Row] = CollectionUtil.iteratorToList(result.collect)
    assertThat(actualRows, containsInAnyOrder(expectedRows: _*))
  }

  private def testResult[T](dataStream: DataStream[T], expectedResult: T*): Unit = {
    var iterator: CloseableIterator[T] = null
    try {
      iterator = dataStream.executeAndCollect()
      val list: util.List[T] = iterator.toList.asJava
      assertThat(list, containsInAnyOrder(expectedResult: _*))
    } finally {
      if (iterator != null) {
        iterator.close()
      }
    }
  }
}

object DataStreamScalaITCase {

  case class ComplexCaseClass(var c: Int, var a: String, var p: ImmutableCaseClass)

  case class ImmutableCaseClass(d: java.lang.Double, b: Boolean)
}
