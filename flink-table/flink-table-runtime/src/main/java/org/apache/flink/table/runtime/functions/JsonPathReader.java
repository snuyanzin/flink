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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.StringUtf8Utils;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonValueContext;

import org.apache.flink.shaded.com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a JSON document like {@link SqlJsonUtils#jsonParse}, but builds only what fixed paths
 * reach: the value at the end of a path in full, and the objects and arrays on the way with only
 * the members the paths need. An array keeps its length, with null for the elements no path needs,
 * so that JsonPath evaluates the paths on the result as on the whole document.
 *
 * <p>A document this reader fails on is parsed by {@link SqlJsonUtils#jsonParse}, which reports the
 * failure as before.
 */
@Internal
public final class JsonPathReader implements Serializable {

    private static final long serialVersionUID = 1L;

    // a property or an index of a compiled JsonPath, as it prints them
    private static final Pattern STEP =
            Pattern.compile("\\['([^'\\\\]*)']|\\[(0|[1-9][0-9]{0,8})]");

    private final Node root = new Node();

    /**
     * @param paths the steps of each path, see {@link #steps}
     */
    public JsonPathReader(Object[][] paths) {
        for (Object[] path : paths) {
            Node node = root;
            for (Object step : path) {
                node = node.children.computeIfAbsent(step, ignored -> new Node());
            }
            node.isEnd = true;
        }
    }

    /**
     * The steps of a JSONPath that JsonPath reads as properties and indices from {@code $} only: a
     * {@link String} for a property, an {@link Integer} for an index. Null for any other path.
     */
    @Nullable
    public static Object[] steps(String path) {
        final String compiled;
        try {
            // as JsonPath reads it, like "$['address']['city']" for "$.address.city"
            compiled = JsonPath.compile(path).getPath();
        } catch (RuntimeException e) {
            return null;
        }
        if (!compiled.startsWith("$")) {
            return null;
        }
        final List<Object> steps = new ArrayList<>();
        final Matcher matcher = STEP.matcher(compiled);
        int position = 1;
        while (position < compiled.length()) {
            if (!matcher.find(position) || matcher.start() != position) {
                return null;
            }
            if (matcher.group(1) != null) {
                steps.add(matcher.group(1));
            } else {
                steps.add(Integer.parseInt(matcher.group(2)));
            }
            position = matcher.end();
        }
        return steps.toArray();
    }

    /** Like {@link #steps}, for a path after its optional {@code lax} or {@code strict}. */
    @Nullable
    public static Object[] stepsAfterMode(String pathSpec) {
        final Matcher matcher = SqlJsonUtils.JSON_PATH_BASE.matcher(pathSpec);
        return steps(matcher.matches() ? matcher.group(2) : pathSpec);
    }

    /**
     * Parses {@code json} for the paths, from its UTF-8 bytes unless it has a Java string already.
     */
    public JsonValueContext parse(StringData json) {
        final BinaryStringData string = (BinaryStringData) json;
        if (string.getJavaObject() != null) {
            // a lone surrogate of the string is encoded as '?', so the string is read as it is
            return parse(string.toString());
        }
        final MemorySegment[] segments = string.getSegments();
        final int offset;
        final byte[] bytes;
        final int size = string.getSizeInBytes();
        // null off the heap
        final byte[] heap = segments[0].getHeapMemory();
        if (heap != null && string.getOffset() + size <= segments[0].size()) {
            bytes = heap;
            offset = string.getOffset();
        } else {
            bytes = string.toBytes();
            offset = 0;
        }
        // bytes the decoder of the full parse replaces by U+FFFD may be read differently here
        if (!exceedsStringLength(size)
                && StringUtf8Utils.firstInvalidUtf8ByteIndex(bytes, offset, size) < 0) {
            try (JsonParser parser = SqlJsonUtils.JSON_FACTORY.createParser(bytes, offset, size)) {
                final JsonValueContext parsed = read(parser);
                if (parsed != null) {
                    return parsed;
                }
            } catch (Exception e) {
                // reported by the full parse
            }
        }
        return SqlJsonUtils.jsonParse(string.toString());
    }

    /** Parses {@code json} for the paths. */
    public JsonValueContext parse(String json) {
        if (!exceedsStringLength(json.length())) {
            try (JsonParser parser = SqlJsonUtils.JSON_FACTORY.createParser(json)) {
                final JsonValueContext parsed = read(parser);
                if (parsed != null) {
                    return parsed;
                }
            } catch (Exception e) {
                // reported by the full parse
            }
        }
        return SqlJsonUtils.jsonParse(json);
    }

    // a string this long may be skipped here, while the whole document fails on it; a document
    // has no more characters than bytes
    private static boolean exceedsStringLength(int length) {
        return length > SqlJsonUtils.JSON_FACTORY.streamReadConstraints().getMaxStringLength();
    }

    // null for a document without content
    @Nullable
    private JsonValueContext read(JsonParser parser) throws Exception {
        final JsonToken token = parser.nextToken();
        // like the full parse, what follows the root value isn't read
        return token == null ? null : JsonValueContext.withJavaObj(read(parser, token, root));
    }

    @Nullable
    private static Object read(JsonParser parser, JsonToken token, Node node) throws Exception {
        if (node.isEnd || (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY)) {
            return SqlJsonUtils.VALUE_READER.readValue(parser);
        }
        if (token == JsonToken.START_OBJECT) {
            final Map<String, Object> object = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String name = parser.currentName();
                final JsonToken value = parser.nextToken();
                final Node child = node.children.get(name);
                if (child == null) {
                    parser.skipChildren();
                } else {
                    // the last of duplicate names wins, as in the full parse
                    object.put(name, read(parser, value, child));
                }
            }
            return object;
        }
        final List<Object> array = new ArrayList<>();
        JsonToken element;
        while ((element = parser.nextToken()) != JsonToken.END_ARRAY) {
            final Node child = node.children.get(array.size());
            if (child == null) {
                parser.skipChildren();
                array.add(null);
            } else {
                array.add(read(parser, element, child));
            }
        }
        return array;
    }

    /** A step of the paths. */
    private static final class Node implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<Object, Node> children = new HashMap<>();
        // a path ends here, so the value is built in full
        private boolean isEnd;
    }
}
