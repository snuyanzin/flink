/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Collections;

/**
 * Test .
 */
public class Flink8255ITCase extends AbstractTestBase {

	@Test
	public void testProgram() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.INT};

		String[] fieldNames = new String[]{"id", "value"};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

		env.fromCollection(Collections.singleton(new Row(2)), rowTypeInfo)
			.keyBy("id").sum("value").print();

		env.execute("Streaming WordCount");
	}

	@Test
	public void testProgram2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.INT};

		String[] fieldNames = new String[]{"id", "value"};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

		UnsortedGrouping groupDs = env.fromCollection(Collections.singleton(new Row(2)), rowTypeInfo).groupBy(0);

		groupDs.maxBy(1);
	}

	@Test
	public void testProgram3() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.INT};

		String[] fieldNames = new String[]{"id", "value"};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

		UnsortedGrouping groupDs = env.fromCollection(Collections.singleton(new Row(2)), rowTypeInfo).groupBy(0);

		groupDs.minBy(1);
	}
}

