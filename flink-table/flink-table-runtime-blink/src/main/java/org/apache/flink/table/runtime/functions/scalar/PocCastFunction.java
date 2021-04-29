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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.inference.CallContext;

import javax.annotation.Nullable;

import java.io.Serializable;


public class PocCastFunction extends BuiltInScalarFunction {
    private CallContext.ProvidedFunction f;
    private MyCast myCast;
    public PocCastFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.POC_CAST, context);
        /*f = ApiExpressionUtils.unresolvedCall(
                BuiltInFunctionDefinitions.CAST,
                Arrays.asList(
                        ApiExpressionUtils.typeLiteral(DataTypes.INT()),
                        ApiExpressionUtils.typeLiteral(DataTypes.INT())));*/

        myCast = (MyCast) UserDefinedFunctionHelper
                .instantiateFunction(MyCast.class);
        Object o = UserDefinedFunctionHelper.createSpecializedFunction("CAST",
                BuiltInFunctionDefinitions.CAST, context.getCallContext(), context.getBuiltInClassLoader(), context.getConfiguration());

        f = context.getCallContext().getProvidedFunction();
    }

    public @Nullable
    Object eval(Object input, Object nullReplacement) {

        Object evaluatedValue = myCast.eval(input);
        if (input == null) {
            return nullReplacement;
        }
        return input;
    }

    public static class MyCast extends ScalarFunction implements Serializable {

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
        }

        public String eval(Object c) {

            return "$" + c;
        }
    }
}
