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
package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.{Before, Test}

class BroadcastHashJoinTest extends JoinTestBase {

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(Long.MaxValue))
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, ShuffleHashJoin")
  }

  @Test
  override def testInnerJoinWithoutJoinPred(): Unit = {
    assertThatThrownBy(() => super.testInnerJoinWithoutJoinPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testLeftOuterJoinNoEquiPred(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinNoEquiPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testLeftOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinOnTrue())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testLeftOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinOnFalse())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testRightOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinOnTrue())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testRightOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinOnFalse())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testRightOuterJoinWithNonEquiPred(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinWithNonEquiPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinWithEquiPred(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinWithEquiPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinWithEquiAndLocalPred(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinWithEquiAndLocalPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinWithEquiAndNonEquiPred(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinWithEquiAndNonEquiPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinWithNonEquiPred(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinWithNonEquiPred())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinOnFalse())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterWithUsing(): Unit = {
    assertThatThrownBy(() => super.testFullOuterWithUsing())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testFullOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinOnTrue())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }

  @Test
  override def testCrossJoin(): Unit = {
    assertThatThrownBy(() => super.testCrossJoin())
      .isInstanceOf(classOf[TableException])
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
  }
}
