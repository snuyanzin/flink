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

package org.apache.flink.api.common.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter;
import org.junit.jupiter.engine.extension.ExtensionRegistry;

import java.util.Arrays;
import java.util.Optional;

/** test. */
public class AfterBeforeParameterResolver implements BeforeEachMethodAdapter, ParameterResolver {
    private ParameterResolver parameterisedTestParameterResolver = null;

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (Arrays.stream(parameterContext.getDeclaringExecutable().getDeclaredAnnotations())
                .anyMatch(
                        t ->
                                t.annotationType() == BeforeEach.class
                                        || t.annotationType() == AfterEach.class)) {
            ParameterContext pContext = getMappedContext(parameterContext, extensionContext);
            return parameterisedTestParameterResolver.supportsParameter(pContext, extensionContext);
        }
        return false;
    }

    private MappedParameterContext getMappedContext(
            ParameterContext parameterContext, ExtensionContext extensionContext) {
        return new MappedParameterContext(
                parameterContext.getIndex(),
                extensionContext.getRequiredTestMethod()
                        .getParameters()[parameterContext.getIndex()],
                Optional.of(parameterContext.getTarget()));
    }

    @Override
    public void invokeBeforeEachMethod(
            ExtensionContext extensionContext, ExtensionRegistry extensionRegistry)
            throws Throwable {
        Optional<ParameterResolver> resolverOptional =
                extensionRegistry.getExtensions(ParameterResolver.class).stream()
                        .filter(
                                parameterResolver ->
                                        parameterResolver
                                                .getClass()
                                                .getName()
                                                .contains("ParameterizedTestParameterResolver"))
                        .findFirst();
        if (!resolverOptional.isPresent()) {
            throw new IllegalStateException(
                    "ParameterizedTestParameterResolver missed in the registry. Probably it's not a Parameterized Test");
        } else {
            parameterisedTestParameterResolver = resolverOptional.get();
        }
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterisedTestParameterResolver.resolveParameter(
                getMappedContext(parameterContext, extensionContext), extensionContext);
    }
}
