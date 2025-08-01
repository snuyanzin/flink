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

package org.apache.flink.streaming.graph;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test translation of {@link CheckpointingMode}. */
@SuppressWarnings("serial")
class TranslationTest {

    @Test
    void testCheckpointModeTranslation() {
        // with deactivated fault tolerance, the checkpoint mode should be at-least-once
        StreamExecutionEnvironment deactivated = getSimpleJob();

        assertThat(
                        CheckpointingOptions.getCheckpointingMode(
                                deactivated.getStreamGraph().getJobGraph().getJobConfiguration()))
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);

        // with activated fault tolerance, the checkpoint mode should be by default exactly once
        StreamExecutionEnvironment activated = getSimpleJob();
        activated.enableCheckpointing(1000L);
        assertThat(
                        CheckpointingOptions.getCheckpointingMode(
                                activated.getStreamGraph().getJobGraph().getJobConfiguration()))
                .isEqualTo(CheckpointingMode.EXACTLY_ONCE);

        // explicitly setting the mode
        StreamExecutionEnvironment explicit = getSimpleJob();
        explicit.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
        assertThat(
                        CheckpointingOptions.getCheckpointingMode(
                                explicit.getStreamGraph().getJobGraph().getJobConfiguration()))
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
    }

    private static StreamExecutionEnvironment getSimpleJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(1, 10000000)
                .addSink(
                        new SinkFunction<Long>() {
                            @Override
                            public void invoke(Long value) {}
                        });

        return env;
    }
}
