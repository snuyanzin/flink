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

import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for {@link BinaryInputFormat} and {@link BinaryOutputFormat}. */
@ExtendWith(AfterBeforeParameterResolver.class)
public abstract class SequentialFormatTestBase<T> {

    private class InputSplitSorter implements Comparator<FileInputSplit> {
        @Override
        public int compare(FileInputSplit o1, FileInputSplit o2) {
            int pathOrder = o1.getPath().getName().compareTo(o2.getPath().getName());
            return pathOrder == 0 ? Long.signum(o1.getStart() - o2.getStart()) : pathOrder;
        }
    }

    private int numberOfTuples;

    protected long blockSize;

    private int parallelism;

    private int[] rawDataSizes;

    @TempDir protected java.nio.file.Path tmpDir;

    /** Count how many bytes would be written if all records were directly serialized. */
    @BeforeEach
    public void calcRawDataSize(int numberOfTuples, long blockSize, int parallelism) throws IOException {
        int recordIndex = 0;
        this.numberOfTuples = numberOfTuples;
        this.blockSize = blockSize;
        this.parallelism = parallelism;
        this.rawDataSizes = new int[parallelism];
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            ByteCounter byteCounter = new ByteCounter();

            for (int fileCount = 0;
                    fileCount < this.getNumberOfTuplesPerFile(fileIndex);
                    fileCount++, recordIndex++) {
                writeRecord(
                        this.getRecord(recordIndex), new DataOutputViewStreamWrapper(byteCounter));
            }
            this.rawDataSizes[fileIndex] = byteCounter.getLength();
        }
    }

    /** Checks if the expected input splits were created. */
    @ParameterizedTest
    @MethodSource("getParameters")
    public void checkInputSplits(int numberOfTuples, long blockSize, int parallelism) throws IOException {
        FileInputSplit[] inputSplits = this.createInputFormat().createInputSplits(0);
        Arrays.sort(inputSplits, new InputSplitSorter());

        int splitIndex = 0;
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            List<FileInputSplit> sameFileSplits = new ArrayList<FileInputSplit>();
            Path lastPath = inputSplits[splitIndex].getPath();
            for (; splitIndex < inputSplits.length; splitIndex++) {
                if (!inputSplits[splitIndex].getPath().equals(lastPath)) {
                    break;
                }
                sameFileSplits.add(inputSplits[splitIndex]);
            }

            assertThat(sameFileSplits).hasSize(this.getExpectedBlockCount(fileIndex));

            long lastBlockLength =
                    this.rawDataSizes[fileIndex] % (this.blockSize - getInfoSize()) + getInfoSize();
            for (int index = 0; index < sameFileSplits.size(); index++) {
                assertThat(sameFileSplits.get(index).getStart()).isEqualTo(this.blockSize * index);
                if (index < sameFileSplits.size() - 1) {
                    assertThat(sameFileSplits.get(index).getLength()).isEqualTo(this.blockSize);
                }
            }
            assertThat(sameFileSplits.get(sameFileSplits.size() - 1).getLength())
                    .isEqualTo(lastBlockLength);
        }
    }

    /** Tests if the expected sequence and amount of data can be read. */
    @ParameterizedTest
    @MethodSource("getParameters")
    public void checkRead(int numberOfTuples, long blockSize, int parallelism) throws Exception {
        BinaryInputFormat<T> input = this.createInputFormat();
        FileInputSplit[] inputSplits = input.createInputSplits(0);
        Arrays.sort(inputSplits, new InputSplitSorter());

        int readCount = 0;

        for (FileInputSplit inputSplit : inputSplits) {
            input.open(inputSplit);
            input.reopen(inputSplit, input.getCurrentState());

            T record = createInstance();

            while (!input.reachedEnd()) {
                if (input.nextRecord(record) != null) {
                    this.checkEquals(this.getRecord(readCount), record);

                    if (!input.reachedEnd()) {
                        Tuple2<Long, Long> state = input.getCurrentState();

                        input = this.createInputFormat();
                        input.reopen(inputSplit, state);
                    }
                    readCount++;
                }
            }
        }
        assertThat(readCount).isEqualTo(numberOfTuples);
    }

    /** Tests the statistics of the given format. */
    @ParameterizedTest
    @MethodSource("getParameters")
    public void checkStatistics(int numberOfTuples, long blockSize, int parallelism) {
        BinaryInputFormat<T> input = this.createInputFormat();
        BaseStatistics statistics = input.getStatistics(null);
        assertThat(statistics.getNumberOfRecords()).isEqualTo(numberOfTuples);
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File subFile : file.listFiles()) {
                this.deleteRecursively(subFile);
            }
        } else {
            file.delete();
        }
    }

    /** Write out the tuples in a temporary file and return it. */
    @BeforeEach
    public void writeTuples(int numberOfTuples, long blockSize, int parallelism) throws IOException {
        this.tempFile = File.createTempFile("BinaryInputFormat", null);
        this.tempFile.deleteOnExit();
        Configuration configuration = new Configuration();
        configuration.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, this.blockSize);
        if (this.parallelism == 1) {
            BinaryOutputFormat<T> output =
                    createOutputFormat(this.tempFile.toURI().toString(), configuration);
            for (int index = 0; index < this.numberOfTuples; index++) {
                output.writeRecord(this.getRecord(index));
            }
            output.close();
        } else {
            this.tempFile.delete();
            this.tempFile.mkdir();
            int recordIndex = 0;
            for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
                BinaryOutputFormat<T> output =
                        createOutputFormat(
                                this.tempFile.toURI() + "/" + (fileIndex + 1), configuration);
                for (int fileCount = 0;
                        fileCount < this.getNumberOfTuplesPerFile(fileIndex);
                        fileCount++, recordIndex++) {
                    output.writeRecord(this.getRecord(recordIndex));
                }
                output.close();
            }
        }
    }

    private int getNumberOfTuplesPerFile(int fileIndex) {
        return this.numberOfTuples / this.parallelism;
    }

    /** Tests if the length of the file matches the expected value. */
    @ParameterizedTest
    @MethodSource("getParameters")
    public void checkLength(int numberOfTuples, long blockSize, int parallelism) {
        File[] files =
                this.tempFile.isDirectory()
                        ? this.tempFile.listFiles()
                        : new File[] {this.tempFile};
        Arrays.sort(files);
        for (int fileIndex = 0; fileIndex < this.parallelism; fileIndex++) {
            long lastBlockLength = this.rawDataSizes[fileIndex] % (this.blockSize - getInfoSize());
            long expectedLength =
                    (this.getExpectedBlockCount(fileIndex) - 1) * this.blockSize
                            + getInfoSize()
                            + lastBlockLength;
            assertThat(files[fileIndex]).hasSize(expectedLength);
        }
    }

    protected abstract BinaryInputFormat<T> createInputFormat();

    protected abstract BinaryOutputFormat<T> createOutputFormat(
            String path, Configuration configuration) throws IOException;

    protected abstract int getInfoSize();

    /** Returns the record to write at the given position. */
    protected abstract T getRecord(int index);

    protected abstract T createInstance();

    protected abstract void writeRecord(T record, DataOutputView outputView) throws IOException;

    /** Checks if both records are equal. */
    protected abstract void checkEquals(T expected, T actual);

    private int getExpectedBlockCount(int fileIndex) {
        int expectedBlockCount =
                (int)
                        Math.ceil(
                                (double) this.rawDataSizes[fileIndex]
                                        / (this.blockSize - getInfoSize()));
        return expectedBlockCount;
    }

    public static Stream<Arguments> getParameters() {
        ArrayList<Arguments> params = new ArrayList<Arguments>();
        for (int parallelism = 1; parallelism <= 2; parallelism++) {
            // numberOfTuples, blockSize, parallelism
            params.add(Arguments.of(100, BinaryOutputFormat.NATIVE_BLOCK_SIZE, parallelism));
            params.add(Arguments.of(100, 1000, parallelism));
            params.add(Arguments.of(100, 1 << 20, parallelism));
            params.add(Arguments.of(10000, 1000, parallelism));
            params.add(Arguments.of(10000, 1 << 20, parallelism));
        }
        return params.stream();
    }

    /** Counts the bytes that would be written. */
    private static final class ByteCounter extends OutputStream {
        int length = 0;

        /**
         * Returns the length.
         *
         * @return the length
         */
        public int getLength() {
            return this.length;
        }

        @Override
        public void write(int b) throws IOException {
            this.length++;
        }
    }
}
