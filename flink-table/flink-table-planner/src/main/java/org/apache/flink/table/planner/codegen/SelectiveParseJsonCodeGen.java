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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.runtime.functions.VariantPathReader;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.variant.Variant;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import scala.Option;

/**
 * Generates {@code PARSE_JSON} and {@code TRY_PARSE_JSON} calls whose result a program only reads
 * at literal {@code [key]} and {@code [index]} paths, with {@link VariantPathReader}, which builds
 * only the values at these paths.
 *
 * <p>A call is read this way only if each use of its result is such a path, and each path is used
 * only by a {@code CAST} to a type other than VARIANT or by {@code JSON_STRING}. Their results
 * don't depend on the field name dictionary, which differs between a value built alone and the same
 * value in the whole document. Any other use, a path with a key or an index that isn't a literal,
 * or an {@code allowDuplicateKeys} argument that isn't a literal, parses the whole document as
 * before.
 */
final class SelectiveParseJsonCodeGen {

    private final CodeGeneratorContext ctx;
    private final RexProgram program;
    private final ExprCodeGenerator exprGenerator;

    // parse local ref -> the parse read selectively
    private final Map<Integer, Parse> parses = new HashMap<>();
    // path local ref -> its parse and its index in the parse's paths
    private final Map<Integer, int[]> paths = new HashMap<>();
    // local refs of a selective parse that nothing reads but its paths
    private final Set<Integer> unused = new HashSet<>();

    SelectiveParseJsonCodeGen(
            CodeGeneratorContext ctx, RexProgram program, ExprCodeGenerator exprGenerator) {
        this.ctx = ctx;
        this.program = program;
        this.exprGenerator = exprGenerator;
        analyze();
    }

    /**
     * The code of a local ref of a selective parse, or empty for any other local ref. A path gets
     * its value from the reader. The parse itself and the steps before a path are never read, so
     * they get no code.
     */
    Optional<GeneratedExpression> generate(int localRef) {
        final int[] path = paths.get(localRef);
        if (path != null) {
            return Optional.of(generatePath(parses.get(path[0]), path[1], localRef));
        }
        if (unused.contains(localRef)) {
            final LogicalType type = typeOf(localRef);
            return Optional.of(
                    new GeneratedExpression(
                            CodeGenUtils.primitiveDefaultValue(type),
                            "true",
                            GeneratedExpression.NO_CODE(),
                            type,
                            Option.empty()));
        }
        return Optional.empty();
    }

    private void analyze() {
        final List<RexNode> exprs = program.getExprList();
        final int inputCount = program.getInputRowType().getFieldCount();
        final Map<Integer, Parse> candidates = new HashMap<>();
        // local ref -> its parse and the steps from it
        final Map<Integer, Chain> chains = new HashMap<>();
        final Set<Integer> fullParses = new HashSet<>();
        // the path local refs, by parse, read by a CAST or JSON_STRING
        final Map<Integer, Set<Integer>> readPaths = new LinkedHashMap<>();

        for (int k = inputCount; k < exprs.size(); k++) {
            final RexNode expr = exprs.get(k);
            if (!(expr instanceof RexCall)) {
                continue;
            }
            final RexCall call = (RexCall) expr;
            final Parse parse = parseOf(call);
            if (parse != null) {
                candidates.put(k, parse);
                chains.put(k, new Chain(k, new Object[0]));
                continue;
            }
            final List<RexNode> operands = call.getOperands();
            for (int i = 0; i < operands.size(); i++) {
                final Chain chain = chainOf(operands.get(i), chains);
                if (chain == null) {
                    continue;
                }
                if (call.getKind() == SqlKind.ITEM && i == 0) {
                    final Object step = literalStep(operands.get(1));
                    if (step == null) {
                        fullParses.add(chain.parse);
                    } else {
                        chains.put(k, chain.then(step));
                    }
                } else if (chain.steps.length > 0 && readsValueOnly(call)) {
                    readPaths
                            .computeIfAbsent(chain.parse, ignored -> new HashSet<>())
                            .add(((RexLocalRef) operands.get(i)).getIndex());
                } else {
                    fullParses.add(chain.parse);
                }
            }
        }
        // the parse or a path as a result of the program
        final List<RexNode> roots = new ArrayList<>(program.getProjectList());
        if (program.getCondition() != null) {
            roots.add(program.getCondition());
        }
        for (RexNode root : roots) {
            final Chain chain = chainOf(root, chains);
            if (chain != null) {
                fullParses.add(chain.parse);
            }
        }

        for (Map.Entry<Integer, Set<Integer>> read : readPaths.entrySet()) {
            final int parseRef = read.getKey();
            if (fullParses.contains(parseRef)) {
                continue;
            }
            final Parse parse = candidates.get(parseRef);
            final Map<List<Object>, Integer> indices = new LinkedHashMap<>();
            for (int pathRef : read.getValue()) {
                final List<Object> steps = Arrays.asList(chains.get(pathRef).steps);
                final int index = indices.computeIfAbsent(steps, ignored -> indices.size());
                paths.put(pathRef, new int[] {parseRef, index});
            }
            parse.paths = indices.keySet().stream().map(List::toArray).toArray(Object[][]::new);
            parses.put(parseRef, parse);
            for (Map.Entry<Integer, Chain> chain : chains.entrySet()) {
                if (chain.getValue().parse == parseRef && !paths.containsKey(chain.getKey())) {
                    unused.add(chain.getKey());
                }
            }
        }
    }

    // PARSE_JSON(json) or PARSE_JSON(json, <literal>), and the same for TRY_PARSE_JSON
    private Parse parseOf(RexCall call) {
        if (!(call.getOperator() instanceof BridgingSqlFunction)) {
            return null;
        }
        final FunctionDefinition definition =
                ((BridgingSqlFunction) call.getOperator()).getDefinition();
        final boolean isTry;
        if (definition == BuiltInFunctionDefinitions.PARSE_JSON) {
            isTry = false;
        } else if (definition == BuiltInFunctionDefinitions.TRY_PARSE_JSON) {
            isTry = true;
        } else {
            return null;
        }
        boolean allowDuplicateKeys = false;
        if (call.getOperands().size() > 1) {
            final RexNode argument = expand(call.getOperands().get(1));
            if (!(argument instanceof RexLiteral)) {
                return null;
            }
            // NULL doesn't allow duplicate keys, as in the full parse
            allowDuplicateKeys =
                    Boolean.TRUE.equals(((RexLiteral) argument).getValueAs(Boolean.class));
        }
        return new Parse(call.getOperands().get(0), isTry, allowDuplicateKeys);
    }

    // a [key] step as a String, an [index] step as an Integer counted from 0, or null
    private Object literalStep(RexNode key) {
        final RexNode node = expand(key);
        if (!(node instanceof RexLiteral) || ((RexLiteral) node).isNull()) {
            return null;
        }
        final RexLiteral literal = (RexLiteral) node;
        if (literal.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
            return literal.getValueAs(String.class);
        }
        if (SqlTypeFamily.INTEGER.getTypeNames().contains(literal.getType().getSqlTypeName())) {
            final long index = literal.getValueAs(Long.class);
            // an index below 1 fails in the full parse's access, which reports it
            return index >= 1 && index - 1 <= Integer.MAX_VALUE ? (int) (index - 1) : null;
        }
        return null;
    }

    // a CAST to a type other than VARIANT, or JSON_STRING
    private static boolean readsValueOnly(RexCall call) {
        if (call.getKind() == SqlKind.CAST) {
            return !FlinkTypeFactory.toLogicalType(call.getType()).is(LogicalTypeRoot.VARIANT);
        }
        return call.getOperator() instanceof BridgingSqlFunction
                && ((BridgingSqlFunction) call.getOperator()).getDefinition()
                        == BuiltInFunctionDefinitions.JSON_STRING;
    }

    private static Chain chainOf(RexNode node, Map<Integer, Chain> chains) {
        return node instanceof RexLocalRef ? chains.get(((RexLocalRef) node).getIndex()) : null;
    }

    private RexNode expand(RexNode node) {
        return node instanceof RexLocalRef ? program.expandLocalRef((RexLocalRef) node) : node;
    }

    private LogicalType typeOf(int localRef) {
        return FlinkTypeFactory.toLogicalType(program.getExprList().get(localRef).getType());
    }

    /*
     * For the path i of a parse, like PARSE_JSON(in)['book']['title']:
     *
     *   // once per parse
     *   StringData pathsInput$0;
     *   Throwable pathsError$0;
     *   private void readPaths$0(StringData in) {
     *     if (in == pathsInput$0) {
     *       return;
     *     }
     *     pathsInput$0 = in;
     *     pathsError$0 = null;
     *     try {
     *       variantPathReader$0.read(in, false);
     *     } catch (Exception e) {
     *       pathsError$0 = e;
     *     }
     *   }
     *
     *   // per use
     *   result$1 = null;
     *   isNull$1 = true;
     *   if (!inIsNull) {
     *     readPaths$0(in);
     *     if (pathsError$0 != null) { // only for PARSE_JSON, not TRY_PARSE_JSON
     *       throw SqlJsonUtils.parseJsonError(in, pathsError$0);
     *     }
     *     if (pathsError$0 == null) {
     *       result$1 = variantPathReader$0.get(i);
     *       isNull$1 = result$1 == null;
     *     }
     *   }
     */
    private GeneratedExpression generatePath(Parse parse, int index, int localRef) {
        final GeneratedExpression input = parse.input.accept(exprGenerator);
        if (parse.readMethod == null) {
            final String reader =
                    ctx.addReusableObject(
                            new VariantPathReader(parse.paths), "variantPathReader", null);
            final String inputType = CodeGenUtils.boxedTypeTermForType(input.resultType());
            parse.readMethod = CodeGenUtils.newName(ctx, "readPaths");
            parse.errorTerm = CodeGenUtils.newName(ctx, "pathsError");
            parse.readerTerm = reader;
            final String lastInput = CodeGenUtils.newName(ctx, "pathsInput");
            ctx.addReusableMember(inputType + " " + lastInput + ";");
            ctx.addReusableMember("Throwable " + parse.errorTerm + ";");
            ctx.addReusableMember(
                    "private void "
                            + parse.readMethod
                            + "("
                            + inputType
                            + " in) {\n"
                            + "  if (in == "
                            + lastInput
                            + ") {\n"
                            + "    return;\n"
                            + "  }\n"
                            + "  "
                            + lastInput
                            + " = in;\n"
                            + "  "
                            + parse.errorTerm
                            + " = null;\n"
                            + "  try {\n"
                            + "    "
                            + reader
                            + ".read(in, "
                            + parse.allowDuplicateKeys
                            + ");\n"
                            + "  } catch (Exception e) {\n"
                            + "    "
                            + parse.errorTerm
                            + " = e;\n"
                            + "  }\n"
                            + "}\n");
        }

        final LogicalType type = typeOf(localRef);
        final String resultTerm =
                ctx.addReusableLocalVariable(CodeGenUtils.className(Variant.class), "result");
        final String nullTerm = ctx.addReusableLocalVariable("boolean", "isNull");
        final String rethrow =
                parse.isTry
                        ? ""
                        : "  if ("
                                + parse.errorTerm
                                + " != null) {\n"
                                + "    throw "
                                + CodeGenUtils.qualifyMethod(BuiltInMethods.PARSE_JSON_ERROR())
                                + "("
                                + input.resultTerm()
                                + ", "
                                + parse.errorTerm
                                + ");\n"
                                + "  }\n";
        final String code =
                input.code()
                        + "\n"
                        + resultTerm
                        + " = null;\n"
                        + nullTerm
                        + " = true;\n"
                        + "if (!"
                        + input.nullTerm()
                        + ") {\n"
                        + "  "
                        + parse.readMethod
                        + "("
                        + input.resultTerm()
                        + ");\n"
                        + rethrow
                        + "  if ("
                        + parse.errorTerm
                        + " == null) {\n"
                        + "    "
                        + resultTerm
                        + " = "
                        + parse.readerTerm
                        + ".get("
                        + index
                        + ");\n"
                        + "    "
                        + nullTerm
                        + " = "
                        + resultTerm
                        + " == null;\n"
                        + "  }\n"
                        + "}\n";
        return new GeneratedExpression(resultTerm, nullTerm, code, type, Option.empty());
    }

    /** A PARSE_JSON call read selectively. */
    private static final class Parse {
        private final RexNode input;
        private final boolean isTry;
        private final boolean allowDuplicateKeys;
        private Object[][] paths;
        // set when the first path is generated
        private String readMethod;
        private String errorTerm;
        private String readerTerm;

        private Parse(RexNode input, boolean isTry, boolean allowDuplicateKeys) {
            this.input = input;
            this.isTry = isTry;
            this.allowDuplicateKeys = allowDuplicateKeys;
        }
    }

    /** The steps from a parse to a local ref. */
    private static final class Chain {
        private final int parse;
        private final Object[] steps;

        private Chain(int parse, Object[] steps) {
            this.parse = parse;
            this.steps = steps;
        }

        private Chain then(Object step) {
            final Object[] next = Arrays.copyOf(steps, steps.length + 1);
            next[steps.length] = step;
            return new Chain(parse, next);
        }
    }
}
