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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class JsonParseReuseTest extends ExpressionTestBase {

  override def testData: Row = {
    val row = new Row(1)
    row.setField(
      0,
      """{"type":"account","age":42,"address":{"city":"Munich"},"roles":["user","viewer"]}""")
    row
  }

  override def containsLegacyTypes: Boolean = true

  override def typeInfo = new RowTypeInfo(Types.STRING)

  private def countOccurrences(code: String, target: String): Int = {
    var count = 0
    var idx = 0
    while ({ idx = code.indexOf(target, idx); idx } >= 0) {
      count += 1
      idx += target.length
    }
    count
  }

  private def assertParseCount(code: String, expected: Int, msg: String): Unit = {
    val actual = countOccurrences(code, "jsonParse(")
    assert(actual == expected, s"$msg: expected $expected but was $actual")
  }

  @Test
  def testTwoJsonValueCallsShareParse(): Unit = {
    val genFunc = getCodeGenFunctions(List("JSON_VALUE(f0, '$.type')", "JSON_VALUE(f0, '$.age')"))
    val code = genFunc.getCode

    assertParseCount(code, 1, "Two JSON_VALUE calls on the same input should parse once")

    val results = evaluateFunctionResult(genFunc)
    assertThat(results(0)).isEqualTo("account")
    assertThat(results(1)).isEqualTo("42")
  }

  @Test
  def testTwoJsonQueryCallsShareParse(): Unit = {
    val genFunc = getCodeGenFunctions(
      List("JSON_QUERY(f0, '$.address')", "JSON_QUERY(f0, '$.roles' WITH WRAPPER)"))
    val code = genFunc.getCode

    assertParseCount(code, 1, "Two JSON_QUERY calls on the same input should parse once")

    val results = evaluateFunctionResult(genFunc)
    assertThat(results(0)).isEqualTo("""{"city":"Munich"}""")
    assertThat(results(1)).isEqualTo("""[["user","viewer"]]""")
  }

  @Test
  def testJsonValueAndJsonQueryShareParse(): Unit = {
    val genFunc = getCodeGenFunctions(
      List("JSON_VALUE(f0, '$.type')", "JSON_QUERY(f0, '$.address')"))
    val code = genFunc.getCode

    assertParseCount(code, 1, "JSON_VALUE + JSON_QUERY on the same input should parse once")

    val results = evaluateFunctionResult(genFunc)
    assertThat(results(0)).isEqualTo("account")
    assertThat(results(1)).isEqualTo("""{"city":"Munich"}""")
  }

  @Test
  def testThreeJsonFunctionsOnSameInputShareParse(): Unit = {
    val genFunc = getCodeGenFunctions(
      List("JSON_VALUE(f0, '$.type')", "JSON_VALUE(f0, '$.age')", "JSON_QUERY(f0, '$.address')"))
    val code = genFunc.getCode

    assertParseCount(code, 1, "Three JSON function calls on the same input should parse once")

    val results = evaluateFunctionResult(genFunc)
    assertThat(results(0)).isEqualTo("account")
    assertThat(results(1)).isEqualTo("42")
    assertThat(results(2)).isEqualTo("""{"city":"Munich"}""")
  }

  @Test
  def testDifferentInputsReturnCorrectResults(): Unit = {
    val genFunc = getCodeGenFunctions(
      List("JSON_VALUE(f0, '$.type')", """JSON_VALUE('{"x":"y"}', '$.x')"""))

    val results = evaluateFunctionResult(genFunc)
    assertThat(results(0)).isEqualTo("account")
    assertThat(results(1)).isEqualTo("y")
  }
}
