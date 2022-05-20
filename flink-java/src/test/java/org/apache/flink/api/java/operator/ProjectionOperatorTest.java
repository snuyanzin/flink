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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link DataSet#project(int...)}. */
public class ProjectionOperatorTest {

    // TUPLE DATA

    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    // LONG DATA

    private final List<Long> emptyLongData = new ArrayList<Long>();

    @Test
    public void testFieldsProjection() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.project(0);
        } catch (Exception e) {
            fail("unknown failure");
        }

        // should not work: too many fields
        assertThatThrownBy(
                        () ->
                                tupleDs.project(
                                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                        17, 18, 19, 20, 21, 22, 23, 24, 25))
                .withFailMessage("unknown failure")
                .isInstanceOf(IllegalArgumentException.class);

        // should not work: index out of bounds of input tuple
        assertThatThrownBy(() -> tupleDs.project(0, 5, 2))
                .withFailMessage("unknown failure")
                .isInstanceOf(IndexOutOfBoundsException.class);

        // should not work: not applied to tuple dataset
        DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
        assertThatThrownBy(() -> longDs.project(0))
                .withFailMessage("unknown failure")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testProjectionTypes() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.project(0);
        } catch (Exception e) {
            fail("unknown failure");
        }

        // should work: dummy types() here
        try {
            tupleDs.project(2, 1, 4);
        } catch (Exception e) {
            fail("unknown failure");
        }
    }

    @Test
    public void testProjectionWithoutTypes() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.project(2, 0, 4);
        } catch (Exception e) {
            fail("unknown failure");
        }

        // should not work: field index is out of bounds of input tuple
        assertThatThrownBy(() -> tupleDs.project(2, -1, 4))
                .withFailMessage("unknown failure")
                .isInstanceOf(IndexOutOfBoundsException.class);

        // should not work: field index is out of bounds of input tuple
        assertThatThrownBy(() -> tupleDs.project(2, 1, 4, 5, 8, 9))
                .withFailMessage("unknown failure")
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
