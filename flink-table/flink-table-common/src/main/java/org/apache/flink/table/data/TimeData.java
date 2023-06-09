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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;

/** TimeData. */
@PublicEvolving
public final class TimeData implements Comparable<TimeData> {
    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    // this field holds the nano-of-millisecond
    private final long nanosecond;

    public TimeData(long nanosecond) {
        this.nanosecond = nanosecond;
    }

    public long getNanosecond() {
        return nanosecond;
    }

    public int getMillisecond() {
        return (int) (nanosecond / 1000_000);
    }

    public static TimeData fromNanos(long nanosecond) {
        return new TimeData(nanosecond);
    }

    @Override
    public int compareTo(TimeData o) {
        return Long.compare(this.nanosecond, o.nanosecond);
    }
}
