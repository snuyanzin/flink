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

package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.{DataTypes, _}
import org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral
import org.apache.flink.table.planner.expressions.utils.MapTypeTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => gLocalTime}

import org.junit.Test

import java.time.{LocalDateTime => JLocalTimestamp}

class MapTypeTest extends MapTypeTestBase {

  @Test
  def testItem(): Unit = {
    testSqlApi("f0['map is null']", "NULL")
    testSqlApi("f1['map is empty']", "NULL")
    testSqlApi("f2['b']", "13")
    testSqlApi("f3[1]", "NULL")
    testSqlApi("f3[12]", "a")
    testSqlApi("f2[f3[12]]", "12")
  }

  @Test
  def testMapField(): Unit = {
    testAllApis(
      map('f4, 'f5),
      "map(f4, f5)",
      "MAP[f4, f5]",
      "{foo=12}")

    testAllApis(
      map('f4, 'f1),
      "map(f4, f1)",
      "MAP[f4, f1]",
      "{foo={}}")

    testAllApis(
      map('f2, 'f3),
      "map(f2, f3)",
      "MAP[f2, f3]",
      "{{a=12, b=13}={12=a, 13=b}}")

    testAllApis(
      map('f1.at("a"), 'f5),
      "map(f1.at('a'), f5)",
      "MAP[f1['a'], f5]",
      "{NULL=12}")

    testAllApis(
      'f1,
      "f1",
      "f1",
      "{}")

    testAllApis(
      'f2,
      "f2",
      "f2",
      "{a=12, b=13}")

    testAllApis(
      'f2.at("a"),
      "f2.at('a')",
      "f2['a']",
      "12")

    testAllApis(
      'f3.at(12),
      "f3.at(12)",
      "f3[12]",
      "a")

    testAllApis(
      map('f4, 'f3).at("foo").at(13),
      "map(f4, f3).at('foo').at(13)",
      "MAP[f4, f3]['foo'][13]",
      "b")
  }

  @Test
  def testMapOperations(): Unit = {

    // comparison
    testAllApis(
      'f1 === 'f2,
      "f1 === f2",
      "f1 = f2",
      "FALSE")

    testAllApis(
      'f3 === 'f7,
      "f3 === f7",
      "f3 = f7",
      "TRUE")

    testAllApis(
      'f5 === 'f2.at("a"),
      "f5 === f2.at('a')",
      "f5 = f2['a']",
      "TRUE")

    testAllApis(
      'f8 === 'f9,
      "f8 === f9",
      "f8 = f9",
      "TRUE")

    testAllApis(
      'f10 === 'f11,
      "f10 === f11",
      "f10 = f11",
      "TRUE")

    testAllApis(
      'f8 !== 'f9,
      "f8 !== f9",
      "f8 <> f9",
      "FALSE")

    testAllApis(
      'f10 !== 'f11,
      "f10 !== f11",
      "f10 <> f11",
      "FALSE")

    testAllApis(
      'f0.at("map is null"),
      "f0.at('map is null')",
      "f0['map is null']",
      "NULL")

    testAllApis(
      'f1.at("map is empty"),
      "f1.at('map is empty')",
      "f1['map is empty']",
      "NULL")

    testAllApis(
      'f2.at("b"),
      "f2.at('b')",
      "f2['b']",
      "13")

    testAllApis(
      'f3.at(1),
      "f3.at(1)",
      "f3[1]",
      "NULL")

    testAllApis(
      'f3.at(12),
      "f3.at(12)",
      "f3[12]",
      "a")

    testAllApis(
      'f3.cardinality(),
      "f3.cardinality()",
      "CARDINALITY(f3)",
      "2")

    testAllApis(
      'f2.at("a").isNotNull,
      "f2.at('a').isNotNull",
      "f2['a'] IS NOT NULL",
      "TRUE")

    testAllApis(
      'f2.at("a").isNull,
      "f2.at('a').isNull",
      "f2['a'] IS NULL",
      "FALSE")

    testAllApis(
      'f2.at("c").isNotNull,
      "f2.at('c').isNotNull",
      "f2['c'] IS NOT NULL",
      "FALSE")

    testAllApis(
      'f2.at("c").isNull,
      "f2.at('c').isNull",
      "f2['c'] IS NULL",
      "TRUE")
  }

}
