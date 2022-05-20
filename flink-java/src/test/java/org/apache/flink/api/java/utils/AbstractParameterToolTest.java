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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Base class for tests for {@link ParameterTool}. */
public abstract class AbstractParameterToolTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Rule public final ExpectedException exception = ExpectedException.none();

    // Test parser

    @Test
    public void testThrowExceptionIfParameterIsNotPrefixed() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Error parsing arguments '[a]' on 'a'. Please prefix keys with -- or -.");

        createParameterToolFromArgs(new String[] {"a"});
    }

    @Test
    public void testNoVal() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-berlin"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(1);
        assertThat(parameter.has("berlin")).isTrue();
    }

    @Test
    public void testNoValDouble() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"--berlin"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(1);
        assertThat(parameter.has("berlin")).isTrue();
    }

    @Test
    public void testMultipleNoVal() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {"--a", "--b", "--c", "--d", "--e", "--f"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(6);
        assertThat(parameter.has("a")).isTrue();
        assertThat(parameter.has("b")).isTrue();
        assertThat(parameter.has("c")).isTrue();
        assertThat(parameter.has("d")).isTrue();
        assertThat(parameter.has("e")).isTrue();
        assertThat(parameter.has("f")).isTrue();
    }

    @Test
    public void testMultipleNoValMixed() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"--a", "-b", "-c", "-d", "--e", "--f"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(6);
        assertThat(parameter.has("a")).isTrue();
        assertThat(parameter.has("b")).isTrue();
        assertThat(parameter.has("c")).isTrue();
        assertThat(parameter.has("d")).isTrue();
        assertThat(parameter.has("e")).isTrue();
        assertThat(parameter.has("f")).isTrue();
    }

    @Test
    public void testEmptyVal() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The input [--a, -b, --] contains an empty argument");

        createParameterToolFromArgs(new String[] {"--a", "-b", "--"});
    }

    @Test
    public void testEmptyValShort() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The input [--a, -b, -] contains an empty argument");

        createParameterToolFromArgs(new String[] {"--a", "-b", "-"});
    }

    // Test unrequested
    // Boolean

    @Test
    public void testUnrequestedBoolean() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedBooleanWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.getBoolean("boolean", false)).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getBoolean("boolean", false)).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedBooleanWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        parameter.getBoolean("boolean");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    // Byte

    @Test
    public void testUnrequestedByte() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        // test parameter access
        assertThat(parameter.getByte("byte")).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getByte("byte")).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedByteWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        // test parameter access
        assertThat(parameter.getByte("byte", (byte) 0)).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getByte("byte", (byte) 0)).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedByteWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getByte("byte");
    }

    // Short

    @Test
    public void testUnrequestedShort() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        // test parameter access
        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedShortWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        // test parameter access
        assertThat(parameter.getShort("short", (short) 0)).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getShort("short", (short) 0)).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedShortWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getShort("short");
    }

    // Int

    @Test
    public void testUnrequestedInt() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        // test parameter access
        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedIntWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        // test parameter access
        assertThat(parameter.getInt("int", 0)).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getInt("int", 0)).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedIntWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getInt("int");
    }

    // Long

    @Test
    public void testUnrequestedLong() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        // test parameter access
        assertThat(parameter.getLong("long")).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getLong("long")).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedLongWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        // test parameter access
        assertThat(parameter.getLong("long", 0)).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getLong("long", 0)).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedLongWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getLong("long");
    }

    // Float

    @Test
    public void testUnrequestedFloat() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        // test parameter access
        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, within(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, within(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedFloatWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        // test parameter access
        assertThat(parameter.getFloat("float", 0.0f)).isCloseTo(4.0f, within(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getFloat("float", 0.0f)).isCloseTo(4.0f, within(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedFloatWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getFloat("float");
    }

    // Double

    @Test
    public void testUnrequestedDouble() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        // test parameter access
        assertThat(parameter.getDouble("double")).isCloseTo(8.0, within(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getDouble("double")).isCloseTo(8.0, within(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedDoubleWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        // test parameter access
        assertThat(parameter.getDouble("double", 0.0)).isCloseTo(8.0, within(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getDouble("double", 0.0)).isCloseTo(8.0, within(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedDoubleWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-double"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getDouble("double");
    }

    // String

    @Test
    public void testUnrequestedString() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        // test parameter access
        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedStringWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        // test parameter access
        assertThat(parameter.get("string", "0.0")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.get("string", "0.0")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedStringWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-string"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        parameter.get("string");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    // Additional methods

    @Test
    public void testUnrequestedHas() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.has("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.has("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedRequired() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-required", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("required");

        // test parameter access
        assertThat(parameter.getRequired("required")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getRequired("required")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedMultiple() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {
                            "-boolean",
                            "true",
                            "-byte",
                            "1",
                            "-short",
                            "2",
                            "-int",
                            "4",
                            "-long",
                            "8",
                            "-float",
                            "4.0",
                            "-double",
                            "8.0",
                            "-string",
                            "∞"
                        });
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder(
                        "boolean", "byte", "short", "int", "long", "float", "double", "string");

        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder(
                        "byte", "short", "int", "long", "float", "double", "string");

        assertThat(parameter.getByte("byte")).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("short", "int", "long", "float", "double", "string");

        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("int", "long", "float", "double", "string");

        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("long", "float", "double", "string");

        assertThat(parameter.getLong("long")).isEqualTo(8L);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("float", "double", "string");

        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, within(0.00001f));
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("double", "string");

        assertThat(parameter.getDouble("double")).isCloseTo(8.0, within(0.00001));
        assertThat(parameter.getUnrequestedParameters()).containsExactlyInAnyOrder("string");

        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    public void testUnrequestedUnknown() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {});
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        assertThat(parameter.getBoolean("boolean", true)).isTrue();
        assertThat(parameter.getByte("byte", (byte) 0)).isEqualTo((byte) 0);
        assertThat(parameter.getShort("short", (short) 0)).isEqualTo((short) 0);
        assertThat(parameter.getInt("int", 0)).isEqualTo(0);
        assertThat(parameter.getLong("long", 0)).isEqualTo(0);
        assertThat(parameter.getFloat("float", 0)).isCloseTo(0f, within(0.00001f));
        assertThat(parameter.getDouble("double", 0)).isCloseTo(0, within(0.00001));
        assertThat(parameter.get("string", "0")).isEqualTo("0");

        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    protected AbstractParameterTool createParameterToolFromArgs(String[] args) {
        return ParameterTool.fromArgs(args);
    }

    protected void validate(AbstractParameterTool parameter) {
        ClosureCleaner.ensureSerializable(parameter);
        internalValidate(parameter);

        // -------- test behaviour after serialization ------------
        try {
            byte[] b = InstantiationUtil.serializeObject(parameter);
            final AbstractParameterTool copy =
                    InstantiationUtil.deserializeObject(b, getClass().getClassLoader());
            internalValidate(copy);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void internalValidate(AbstractParameterTool parameter) {
        assertThat(parameter.getRequired("input")).isEqualTo("myInput");
        assertThat(parameter.get("output", "myDefaultValue")).isEqualTo("myDefaultValue");
        assertThat(parameter.get("whatever")).isNull();
        assertThat(parameter.getLong("expectedCount", -1L)).isEqualTo(15L);
        assertThat(parameter.getBoolean("thisIsUseful", true)).isTrue();
        assertThat(parameter.getByte("myDefaultByte", (byte) 42)).isEqualTo((byte) 42);
        assertThat(parameter.getShort("myDefaultShort", (short) 42)).isEqualTo((short) 42);

        if (parameter instanceof ParameterTool) {
            ParameterTool parameterTool = (ParameterTool) parameter;
            final Configuration config = parameterTool.getConfiguration();
            assertThat(config.getLong("expectedCount", -1L)).isEqualTo(15L);

            final Properties props = parameterTool.getProperties();
            assertThat(props.getProperty("input")).isEqualTo("myInput");

            // -------- test the default file creation ------------
            try {
                final String pathToFile = tmp.newFile().getAbsolutePath();
                parameterTool.createPropertiesFile(pathToFile);
                final Properties defaultProps = new Properties();
                try (FileInputStream fis = new FileInputStream(pathToFile)) {
                    defaultProps.load(fis);
                }

                assertThat(defaultProps)
                        .containsEntry("output", "myDefaultValue")
                        .containsEntry("expectedCount", "-1")
                        .containsKey("input");

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (parameter instanceof MultipleParameterTool) {
            MultipleParameterTool multipleParameterTool = (MultipleParameterTool) parameter;
            List<String> multiValues = Arrays.asList("multiValue1", "multiValue2");
            assertThat(multipleParameterTool.getMultiParameter("multi")).isEqualTo(multiValues);

            assertThat(multipleParameterTool.toMultiMap()).containsEntry("multi", multiValues);

            // The last value is used.
            assertThat(multipleParameterTool.toMap()).containsEntry("multi", "multiValue2");
        }
    }
}
