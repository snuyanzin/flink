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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.util.Visitor;

import java.util.List;

/**
 * Abstract superclass for all operators that have one input like "map" or "reduce".
 *
 * @param <IN> Input type of the user function
 * @param <OUT> Output type of the user function
 * @param <FT> Type of the user function
 */
@Internal
public abstract class SingleInputOperator<IN, OUT, FT extends Function>
        extends AbstractUdfOperator<OUT, FT> {

    /** The input which produces the data consumed by this operator. */
    protected Operator<IN> input;

    /** The positions of the keys in the tuple. */
    private final int[] keyFields;

    /** Semantic properties of the associated function. */
    private SingleInputSemanticProperties semanticProperties = new SingleInputSemanticProperties();

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a new abstract single-input operator with the given name wrapping the given user
     * function.
     *
     * @param stub The object containing the user function.
     * @param keyPositions The field positions of the input records that act as keys.
     * @param name The given name for the operator, used in plans, logs and progress messages.
     */
    protected SingleInputOperator(
            UserCodeWrapper<FT> stub,
            UnaryOperatorInformation<IN, OUT> operatorInfo,
            int[] keyPositions,
            String name) {
        super(stub, operatorInfo, name);
        this.keyFields = keyPositions;
    }

    /**
     * Creates a new abstract single-input operator with the given name wrapping the given user
     * function. This constructor is specialized only for operators that require no keys for their
     * processing.
     *
     * @param stub The object containing the user function.
     * @param name The given name for the operator, used in plans, logs and progress messages.
     */
    protected SingleInputOperator(
            UserCodeWrapper<FT> stub, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
        super(stub, operatorInfo, name);
        this.keyFields = new int[0];
    }

    // --------------------------------------------------------------------------------------------

    /** Gets the information about the operators input/output types. */
    @Override
    @SuppressWarnings("unchecked")
    public UnaryOperatorInformation<IN, OUT> getOperatorInfo() {
        return (UnaryOperatorInformation<IN, OUT>) this.operatorInfo;
    }

    /**
     * Returns the input operator or data source, or null, if none is set.
     *
     * @return This operator's input.
     */
    public Operator<IN> getInput() {
        return this.input;
    }

    /** Removes all inputs. */
    public void clearInputs() {
        this.input = null;
    }

    /**
     * Sets the given operator as the input to this operator.
     *
     * @param input The operator to use as the input.
     */
    public void setInput(Operator<IN> input) {
        this.input = input;
    }

    // --------------------------------------------------------------------------------------------

    public SingleInputSemanticProperties getSemanticProperties() {
        return this.semanticProperties;
    }

    public void setSemanticProperties(SingleInputSemanticProperties semanticProperties) {
        this.semanticProperties = semanticProperties;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public final int getNumberOfInputs() {
        return 1;
    }

    @Override
    public int[] getKeyColumns(int inputNum) {
        if (inputNum == 0) {
            return this.keyFields;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Accepts the visitor and applies it this instance. The visitors pre-visit method is called
     * and, if returning <tt>true</tt>, the visitor is recursively applied on the single input.
     * After the recursion returned, the post-visit method is called.
     *
     * @param visitor The visitor.
     * @see org.apache.flink.util.Visitable#accept(org.apache.flink.util.Visitor)
     */
    @Override
    public void accept(Visitor<Operator<?>> visitor) {
        if (visitor.preVisit(this)) {
            this.input.accept(visitor);
            for (Operator<?> c : this.broadcastInputs.values()) {
                c.accept(visitor);
            }
            visitor.postVisit(this);
        }
    }

    // --------------------------------------------------------------------------------------------

    protected abstract List<OUT> executeOnCollections(
            List<IN> inputData, RuntimeContext runtimeContext, ExecutionConfig executionConfig)
            throws Exception;
}
