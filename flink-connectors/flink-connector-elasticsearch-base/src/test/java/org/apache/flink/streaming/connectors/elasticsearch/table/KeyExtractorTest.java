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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.apache.flink.table.catalog.Column.physical;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyExtractor}. */
public class KeyExtractorTest {
    @Test
    public void testSimpleKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("a", DataTypes.BIGINT().notNull()),
                                physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_1", Collections.singletonList("a")));

        Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

        String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isEqualTo("12");
    }

    @Test
    public void testNoPrimaryKey() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        physical("a", DataTypes.BIGINT().notNull()),
                        physical("b", DataTypes.STRING()));

        Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

        String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isNull();
    }

    @Test
    public void testTwoFieldsKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("a", DataTypes.BIGINT().notNull()),
                                physical("b", DataTypes.STRING()),
                                physical("c", DataTypes.TIMESTAMP().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_1", Arrays.asList("a", "c")));

        Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

        String key =
                keyExtractor.apply(
                        GenericRowData.of(
                                12L,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12"))));
        assertThat(key).isEqualTo("12_2012-12-12T12:12:12");
    }

    @Test
    public void testAllTypesKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                physical("a", DataTypes.TINYINT().notNull()),
                                physical("b", DataTypes.SMALLINT().notNull()),
                                physical("c", DataTypes.INT().notNull()),
                                physical("d", DataTypes.BIGINT().notNull()),
                                physical("e", DataTypes.BOOLEAN().notNull()),
                                physical("f", DataTypes.FLOAT().notNull()),
                                physical("g", DataTypes.DOUBLE().notNull()),
                                physical("h", DataTypes.STRING().notNull()),
                                physical("i", DataTypes.TIMESTAMP().notNull()),
                                physical("j", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull()),
                                physical("k", DataTypes.TIME().notNull()),
                                physical("l", DataTypes.DATE().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "PK_1",
                                Arrays.asList(
                                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                                        "l")));

        Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

        String key =
                keyExtractor.apply(
                        GenericRowData.of(
                                (byte) 1,
                                (short) 2,
                                3,
                                (long) 4,
                                true,
                                1.0f,
                                2.0d,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12")),
                                TimestampData.fromInstant(Instant.parse("2013-01-13T13:13:13Z")),
                                (int) (LocalTime.parse("14:14:14").toNanoOfDay() / 1_000_000),
                                (int) LocalDate.parse("2015-05-15").toEpochDay()));
        assertThat(key)
                .isEqualTo(
                        "1_2_3_4_true_1.0_2.0_ABCD_2012-12-12T12:12:12_2013-01-13T13:13:13_14:14:14_2015-05-15");
    }
}
