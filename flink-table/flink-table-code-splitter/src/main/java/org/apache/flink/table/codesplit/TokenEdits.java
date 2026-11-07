/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Replacement for {@link org.antlr.v4.runtime.TokenStreamRewriter} with the same output for the
 * operations used by the code splitter. {@code TokenStreamRewriter#getText} is quadratic in the
 * number of edits, here edits are applied when recorded and {@link #getText} is a single pass.
 *
 * <p>As in {@code TokenStreamRewriter}: a replace drops earlier replaces it contains and earlier
 * inserts strictly inside it, overlapping deletes are merged, other overlaps and inserts into a
 * replaced range throw, inserts at the start of a replace are emitted before it in reverse order.
 */
final class TokenEdits {

    private final CommonTokenStream tokens;
    private final int size;
    // replace starting at a token: its last token index (-1 if none) and text (null for delete)
    private final int[] replaceStop;
    private final String[] replaceText;
    // start of the replace covering a token, -1 if none
    private final int[] coveredBy;
    // inserts before a token in instruction order, the last slot is after EOF
    private final List<List<Insert>> inserts;

    TokenEdits(CommonTokenStream tokens) {
        tokens.fill();
        this.tokens = tokens;
        this.size = tokens.size();
        this.replaceStop = new int[size];
        this.replaceText = new String[size];
        this.coveredBy = new int[size];
        this.inserts = new ArrayList<>(Collections.nCopies(size + 1, null));
        Arrays.fill(replaceStop, -1);
        Arrays.fill(coveredBy, -1);
    }

    void insertBefore(Token token, Object text) {
        insert(token.getTokenIndex(), String.valueOf(text), false);
    }

    void insertAfter(Token token, Object text) {
        insert(token.getTokenIndex() + 1, String.valueOf(text), true);
    }

    void replace(Token token, Object text) {
        replace(token, token, text);
    }

    void replace(Token start, Token stop, Object text) {
        replace(start.getTokenIndex(), stop.getTokenIndex(), String.valueOf(text));
    }

    void delete(Token start, Token stop) {
        replace(start.getTokenIndex(), stop.getTokenIndex(), null);
    }

    String getText() {
        return getText(0, size - 1);
    }

    String getText(Interval interval) {
        return getText(interval.a, interval.b);
    }

    private void insert(int index, String text, boolean after) {
        if (index < size && coveredBy[index] != -1 && coveredBy[index] != index) {
            throw new IllegalArgumentException(
                    "insert op at " + index + " within boundaries of previous replace");
        }
        if (inserts.get(index) == null) {
            inserts.set(index, new ArrayList<>(1));
        }
        inserts.get(index).add(new Insert(text, after));
    }

    private void replace(int start, int stop, String text) {
        for (int i = start; i <= stop; i++) {
            int prevStart = coveredBy[i];
            if (prevStart == -1) {
                continue;
            }
            int prevStop = replaceStop[prevStart];
            boolean bothDeletes = text == null && replaceText[prevStart] == null;
            if (prevStart >= start && prevStop <= stop) {
                dropReplace(prevStart);
            } else if (bothDeletes) {
                dropReplace(prevStart);
                replace(Math.min(start, prevStart), Math.max(stop, prevStop), null);
                return;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "replace op boundaries of %d..%d overlap with previous %d..%d",
                                start, stop, prevStart, prevStop));
            }
        }
        for (int i = start + 1; i <= stop; i++) {
            inserts.set(i, null);
        }
        Arrays.fill(coveredBy, start, stop + 1, start);
        replaceStop[start] = stop;
        replaceText[start] = text;
    }

    private void dropReplace(int start) {
        Arrays.fill(coveredBy, start, replaceStop[start] + 1, -1);
        replaceStop[start] = -1;
        replaceText[start] = null;
    }

    private String getText(int start, int stop) {
        start = Math.max(start, 0);
        stop = Math.min(stop, size - 1);
        StringBuilder sb = new StringBuilder();
        int i = start;
        while (i <= stop) {
            if (replaceStop[i] != -1) {
                appendInsertsReversed(sb, i);
                if (replaceText[i] != null) {
                    sb.append(replaceText[i]);
                }
                i = replaceStop[i] + 1;
            } else {
                appendInserts(sb, i);
                Token token = tokens.get(i);
                if (token.getType() != Token.EOF) {
                    sb.append(token.getText());
                }
                i++;
            }
        }
        if (stop == size - 1) {
            appendInserts(sb, size);
        }
        return sb.toString();
    }

    /** Combines inserts at one index the way {@code TokenStreamRewriter} does. */
    private void appendInserts(StringBuilder sb, int index) {
        List<Insert> list = inserts.get(index);
        if (list == null) {
            return;
        }
        StringBuilder combined = new StringBuilder(list.get(0).text);
        for (int j = 1; j < list.size(); j++) {
            String text = list.get(j).text;
            if (list.get(j - 1).after) {
                combined.append(text);
            } else {
                combined.insert(0, text);
            }
        }
        sb.append(combined);
    }

    private void appendInsertsReversed(StringBuilder sb, int index) {
        List<Insert> list = inserts.get(index);
        if (list == null) {
            return;
        }
        for (int j = list.size() - 1; j >= 0; j--) {
            sb.append(list.get(j).text);
        }
    }

    private static final class Insert {
        final String text;
        final boolean after;

        Insert(String text, boolean after) {
            this.text = text;
            this.after = after;
        }
    }
}
