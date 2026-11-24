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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.SwarUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads the values at fixed paths of a JSON document, as {@code PARSE_JSON(json)} followed by
 * {@code [key]} and {@code [index]} access would, but builds only those values. The rest of the
 * document is read without being built, and is still checked as {@code PARSE_JSON} checks it, so
 * that a document fails the same way.
 *
 * <p>A built value has a field name dictionary of its own, so it must not be compared with variants
 * that come from the whole document. The variant size limit applies to the built values only, not
 * to the whole document as in {@code PARSE_JSON}.
 */
@Internal
public final class VariantPathReader implements Serializable {

    private static final long serialVersionUID = 1L;

    // the errors of [key] and [index] access, see ScalarOperatorGens.generateVariantGet
    private static final String KEY_ON_NON_OBJECT =
            "String key access on variant requires an object variant, but a non-object variant was provided.";
    private static final String INDEX_ON_NON_ARRAY =
            "Integer index access on variant requires an array variant, but a non-array variant was provided.";

    private final Node root = new Node();
    private final int numPaths;

    private transient Variant[] values;
    private transient String[] errors;
    // An unpaired surrogate in a key can only come from a \\u escape, so without a backslash in the
    // document the keys are compared as they are.
    private transient boolean hasEscapes;
    // the keys of the objects being read, by depth, to find duplicates without a set per object
    private transient List<ObjectKeys> keysByDepth;

    /**
     * @param paths the steps of each path: a {@link String} for {@code [key]}, or an {@link
     *     Integer} for {@code [index]}, counted from 0
     */
    public VariantPathReader(Object[][] paths) {
        this.numPaths = paths.length;
        for (int i = 0; i < paths.length; i++) {
            Node node = root;
            for (Object step : paths[i]) {
                node = node.child(step);
            }
            node.ends.add(i);
        }
        root.collectPaths();
    }

    /**
     * Reads the paths of {@code json}. Throws what {@code PARSE_JSON(json)} throws for it, as
     * {@link SqlJsonUtils#parseJsonToVariant} does, except for the size limit of the whole
     * document.
     */
    public void read(StringData json, boolean allowDuplicateKeys) throws IOException {
        if (values == null) {
            values = new Variant[numPaths];
            errors = new String[numPaths];
            keysByDepth = new ArrayList<>();
        }
        root.clear(this);
        final byte[] bytes = json.toBytes();
        hasEscapes = SwarUtils.contains(bytes, 0, bytes.length, (byte) '\\');
        try (JsonParser parser = BinaryVariantInternalBuilder.createParser(bytes)) {
            parser.nextToken();
            if (parser.currentToken() == null) {
                // fails as the full parse does
                BinaryVariantInternalBuilder.parseJson(parser, allowDuplicateKeys);
            }
            visit(parser, root, allowDuplicateKeys, 0);
        }
    }

    /**
     * The value at the path read last, or null if a key or an index of the path is missing.
     *
     * @throws TableRuntimeException if a step of the path accesses a value of the wrong type, as
     *     {@code [key]} or {@code [index]} access does
     */
    public @Nullable Variant get(int path) {
        if (errors[path] != null) {
            throw new TableRuntimeException(errors[path]);
        }
        return values[path];
    }

    // the parser is at the first token of the value at node
    private void visit(JsonParser parser, Node node, boolean allowDuplicateKeys, int depth)
            throws IOException {
        if (!node.ends.isEmpty()) {
            // the value is needed as a whole, and deeper paths are read from it
            resolve(node, BinaryVariantInternalBuilder.parseJson(parser, allowDuplicateKeys));
            return;
        }
        final JsonToken token = parser.currentToken();
        if (token == JsonToken.START_OBJECT) {
            node.failElements(this);
            final ObjectKeys keys = allowDuplicateKeys ? null : keysAt(depth);
            boolean duplicate = false;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String key = keyOf(parser.currentName());
                parser.nextToken();
                if (keys != null && !keys.add(key)) {
                    duplicate = true;
                }
                final Node child = node.fields.get(key);
                if (child != null) {
                    // with duplicate keys, the last one wins, as in the full parse
                    child.clear(this);
                    visit(parser, child, allowDuplicateKeys, depth + 1);
                } else {
                    skip(parser, allowDuplicateKeys, depth + 1);
                }
            }
            if (duplicate) {
                // reported when the object ends, as the full parse reports it
                throw BinaryVariantInternalBuilder.VARIANT_DUPLICATE_KEY_EXCEPTION;
            }
        } else if (token == JsonToken.START_ARRAY) {
            node.failFields(this);
            int index = 0;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                final Node child = node.elements.get(index++);
                if (child != null) {
                    visit(parser, child, allowDuplicateKeys, depth + 1);
                } else {
                    skip(parser, allowDuplicateKeys, depth + 1);
                }
            }
        } else {
            node.failFields(this);
            node.failElements(this);
            skip(parser, allowDuplicateKeys, depth);
        }
    }

    // reads the value at the parser without building it, and fails where building it fails
    private void skip(JsonParser parser, boolean allowDuplicateKeys, int depth) throws IOException {
        final JsonToken token = parser.currentToken();
        switch (token) {
            case START_OBJECT:
                final ObjectKeys keys = allowDuplicateKeys ? null : keysAt(depth);
                boolean duplicate = false;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    final String key = parser.currentName();
                    parser.nextToken();
                    if (keys != null && !keys.add(keyOf(key))) {
                        duplicate = true;
                    }
                    skip(parser, allowDuplicateKeys, depth + 1);
                }
                if (duplicate) {
                    throw BinaryVariantInternalBuilder.VARIANT_DUPLICATE_KEY_EXCEPTION;
                }
                break;
            case START_ARRAY:
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    skip(parser, allowDuplicateKeys, depth + 1);
                }
                break;
            case VALUE_STRING:
                // decodes the string, so that invalid UTF-8 and too long strings fail
                parser.getTextCharacters();
                break;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                BinaryVariantInternalBuilder.checkJsonNumber(parser);
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_NULL:
                break;
            default:
                throw new JsonParseException(parser, "Unexpected token " + token);
        }
    }

    // sets the paths at and below node from its built value
    private void resolve(Node node, Variant value) {
        for (int path : node.ends) {
            values[path] = value;
        }
        if (!node.fields.isEmpty()) {
            if (value.isObject()) {
                for (Map.Entry<String, Node> field : node.fields.entrySet()) {
                    final Variant child =
                            ((BinaryVariant) value)
                                    .getFieldByUtf8Key(
                                            field.getKey().getBytes(StandardCharsets.UTF_8));
                    if (child != null) {
                        resolve(field.getValue(), child);
                    }
                }
            } else {
                node.failFields(this);
            }
        }
        if (!node.elements.isEmpty()) {
            if (value.isArray()) {
                for (Map.Entry<Integer, Node> element : node.elements.entrySet()) {
                    final Variant child = value.getElement(element.getKey());
                    if (child != null) {
                        resolve(element.getValue(), child);
                    }
                }
            } else {
                node.failElements(this);
            }
        }
    }

    private String keyOf(String name) {
        return hasEscapes ? storedKey(name) : name;
    }

    // the keys of the object at depth, emptied for a new object
    private ObjectKeys keysAt(int depth) {
        while (keysByDepth.size() <= depth) {
            keysByDepth.add(new ObjectKeys());
        }
        final ObjectKeys keys = keysByDepth.get(depth);
        keys.clear();
        return keys;
    }

    // Keys are compared by their UTF-8 bytes, which turn an unpaired surrogate into '?'. Decoding
    // them again gives one string for keys with the same bytes.
    private static String storedKey(String key) {
        for (int i = 0; i < key.length(); i++) {
            if (Character.isSurrogate(key.charAt(i))) {
                return new String(key.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            }
        }
        return key;
    }

    /** The keys of an object, reused for the objects at the same depth. */
    private static final class ObjectKeys {

        // compared one by one, so that a small object needs no hashing
        private static final int SCANNED = 16;

        private final String[] scanned = new String[SCANNED];
        private final Set<String> hashed = new HashSet<>();
        private int count;

        private void clear() {
            count = 0;
            if (!hashed.isEmpty()) {
                hashed.clear();
            }
        }

        // whether key is new in the object, which it then holds
        private boolean add(String key) {
            if (count < SCANNED) {
                for (int i = 0; i < count; i++) {
                    if (scanned[i].equals(key)) {
                        return false;
                    }
                }
                scanned[count++] = key;
                return true;
            }
            if (count == SCANNED) {
                hashed.addAll(Arrays.asList(scanned));
            }
            count++;
            return hashed.add(key);
        }
    }

    /** A step of the paths, with the paths that end at it and the steps that follow it. */
    private static final class Node implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, Node> fields = new HashMap<>();
        private final Map<Integer, Node> elements = new HashMap<>();
        private final List<Integer> ends = new ArrayList<>();
        // the paths that end at or below this node
        private int[] paths;
        // the paths that go through a [key] or an [index] step of this node
        private int[] fieldPaths;
        private int[] elementPaths;

        private Node child(Object step) {
            if (step instanceof Integer) {
                return elements.computeIfAbsent((Integer) step, ignored -> new Node());
            }
            return fields.computeIfAbsent(storedKey((String) step), ignored -> new Node());
        }

        private int[] collectPaths() {
            final List<Integer> fieldList = new ArrayList<>();
            for (Node child : fields.values()) {
                for (int path : child.collectPaths()) {
                    fieldList.add(path);
                }
            }
            final List<Integer> elementList = new ArrayList<>();
            for (Node child : elements.values()) {
                for (int path : child.collectPaths()) {
                    elementList.add(path);
                }
            }
            fieldPaths = fieldList.stream().mapToInt(Integer::intValue).toArray();
            elementPaths = elementList.stream().mapToInt(Integer::intValue).toArray();
            final List<Integer> all = new ArrayList<>(ends);
            all.addAll(fieldList);
            all.addAll(elementList);
            paths = all.stream().mapToInt(Integer::intValue).toArray();
            return paths;
        }

        // a value at or below this node may have been set by an earlier value of the same key
        private void clear(VariantPathReader reader) {
            for (int path : paths) {
                reader.values[path] = null;
                reader.errors[path] = null;
            }
        }

        private void failFields(VariantPathReader reader) {
            for (int path : fieldPaths) {
                reader.errors[path] = KEY_ON_NON_OBJECT;
            }
        }

        private void failElements(VariantPathReader reader) {
            for (int path : elementPaths) {
                reader.errors[path] = INDEX_ON_NON_ARRAY;
            }
        }
    }
}
