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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a Flink runtime Task.
 *
 * <p>Contains extra logic for adding operators.
 */
@Internal
public class TaskMetricGroup extends ComponentMetricGroup<TaskManagerJobMetricGroup> {

    private final Map<String, InternalOperatorMetricGroup> operators = new HashMap<>();

    private final TaskIOMetricGroup ioMetrics;

    /**
     * The execution Id uniquely identifying the executed task represented by this metrics group.
     */
    private final ExecutionAttemptID executionId;

    protected final JobVertexID vertexId;

    private final String taskName;

    protected final int subtaskIndex;

    private final int attemptNumber;

    // ------------------------------------------------------------------------

    TaskMetricGroup(
            MetricRegistry registry,
            TaskManagerJobMetricGroup parent,
            ExecutionAttemptID executionId,
            String taskName) {
        super(
                registry,
                registry.getScopeFormats()
                        .getTaskFormat()
                        .formatScope(
                                checkNotNull(parent),
                                checkNotNull(executionId).getJobVertexId(),
                                checkNotNull(executionId),
                                taskName,
                                checkNotNull(executionId).getSubtaskIndex(),
                                checkNotNull(executionId).getAttemptNumber()),
                parent);

        this.executionId = checkNotNull(executionId);
        this.vertexId = executionId.getJobVertexId();
        this.taskName = checkNotNull(taskName);
        this.subtaskIndex = executionId.getSubtaskIndex();
        this.attemptNumber = executionId.getAttemptNumber();

        this.ioMetrics = new TaskIOMetricGroup(this);
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    public final TaskManagerJobMetricGroup parent() {
        return parent;
    }

    public ExecutionAttemptID executionId() {
        return executionId;
    }

    @Nullable
    public AbstractID vertexId() {
        return vertexId;
    }

    @Nullable
    public String taskName() {
        return taskName;
    }

    public int subtaskIndex() {
        return subtaskIndex;
    }

    public int attemptNumber() {
        return attemptNumber;
    }

    /**
     * Returns the TaskIOMetricGroup for this task.
     *
     * @return TaskIOMetricGroup for this task.
     */
    public TaskIOMetricGroup getIOMetricGroup() {
        return ioMetrics;
    }

    @Override
    protected QueryScopeInfo.TaskQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.TaskQueryScopeInfo(
                this.parent.jobId.toString(),
                String.valueOf(this.vertexId),
                this.subtaskIndex,
                this.attemptNumber);
    }

    // ------------------------------------------------------------------------
    //  operators and cleanup
    // ------------------------------------------------------------------------

    public InternalOperatorMetricGroup getOrAddOperator(String operatorName) {
        return getOrAddOperator(
                OperatorID.fromJobVertexID(vertexId), operatorName, Collections.emptyMap());
    }

    public InternalOperatorMetricGroup getOrAddOperator(
            OperatorID operatorID, String operatorName, Map<String, String> additionalVariables) {
        final String truncatedOperatorName = MetricUtils.truncateOperatorName(operatorName);

        // unique OperatorIDs only exist in streaming, so we have to rely on the name for batch
        // operators
        final String key = operatorID + truncatedOperatorName;

        synchronized (this) {
            return operators.computeIfAbsent(
                    key,
                    operator ->
                            new InternalOperatorMetricGroup(
                                    this.registry,
                                    this,
                                    operatorID,
                                    truncatedOperatorName,
                                    additionalVariables));
        }
    }

    @Override
    public void close() {
        super.close();

        parent.removeTaskMetricGroup(executionId);
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_TASK_VERTEX_ID, vertexId.toString());
        variables.put(ScopeFormat.SCOPE_TASK_NAME, taskName);
        variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_ID, executionId.toString());
        variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, String.valueOf(attemptNumber));
        variables.put(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, String.valueOf(subtaskIndex));
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup<?>> subComponents() {
        return operators.values();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "task";
    }
}
