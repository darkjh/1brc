/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.juhanlol;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CalculateAverageSlices {

    private static final String FILE = "./measurements.txt";
    private static final int NUM_THREADS = 4;

    public static void main(String[] args) throws IOException, InterruptedException {
        var file = new RandomAccessFile(FILE, "r");
        var sliceSize = 500 * 1000 * 1000; // 100MB
        var slices = findSlices(file, sliceSize);
        file.close();

        var aggrs = new ArrayList<HashMap<String, double[]>>(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            aggrs.add(new HashMap<>(1000));
        }

        var files = new ArrayList<RandomAccessFile>(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            files.add(new RandomAccessFile(FILE, "r"));
        }

        var pool = Executors.newWorkStealingPool(NUM_THREADS);

        for (var slice : slices) {
            CompletableFuture.runAsync(new Task(slice, aggrs, files), pool);
        }
        pool.shutdown();
        pool.awaitTermination(1000000, TimeUnit.DAYS);

        var result = aggregateAll(aggrs);
        displayResult(result);
    }

  private record Slice(long start, long end, long size) {
    Slice(long start, long end) {
      this(start, end, end - start);
    }
  }

    private static List<Slice> findSlices(RandomAccessFile file, long sliceSize) throws IOException {
        var length = file.length();
        var slices = new ArrayList<Slice>();
        var start = 0L;
        while (start < length) {
            var end = start + sliceSize;
            file.seek(end);
            while ((char) file.read() != '\n' && end <= length) {
                end++;
            }

            var slice = new Slice(start, end);
            slices.add(slice);
            start = end + 1;
        }
        return slices;
    }

    private static Map<String, double[]> aggregateAll(List<HashMap<String, double[]>> aggrs) {
        var all = aggrs.get(0);
        for (int i = 1; i < aggrs.size(); i++) {
            for (var entry : aggrs.get(i).entrySet()) {
                var s = entry.getKey();
                all.compute(s, (_, v) -> {
                    if (v == null) {
                        return entry.getValue();
                    }
                    v[0] = Math.min(v[0], entry.getValue()[0]);
                    v[1] = Math.max(v[1], entry.getValue()[1]);
                    v[2] += entry.getValue()[2];
                    v[3] += entry.getValue()[3];
                    return v;
                });
            }
        }
        return all;
    }

    private static void displayResult(Map<String, double[]> result) {
        var measurements = new TreeMap<String, ResultRow>();
        for (var entry : result.entrySet()) {
            var station = entry.getKey();
            var values = entry.getValue();

            var row = new ResultRow(values[0], values[2] / values[3], values[1]);
            measurements.put(station, row);
        }

        System.out.println(measurements);
        System.out.println(measurements.size());
    }

    private static class ThreadId {
        private static final AtomicInteger nextId = new AtomicInteger(0);

        private static final ThreadLocal<Integer> threadId = ThreadLocal.withInitial(nextId::getAndIncrement);

        public static int get() {
            return threadId.get();
        }
    }

    private static class Task implements Runnable {
        private final Slice slice;
        private final ByteBuffer buffer;
        private final ArrayList<HashMap<String, double[]>> aggrs;
        private final ArrayList<RandomAccessFile> files;

        Task(Slice slice, ArrayList<HashMap<String, double[]>> aggrs, ArrayList<RandomAccessFile> files) {
            this.slice = slice;
            this.aggrs = aggrs;
            this.files = files;
            this.buffer = ByteBuffer.allocate(128);
        }

        @Override
        public void run() {
            var tid = ThreadId.get();
            var aggr = aggrs.get(tid);
            var file = files.get(tid);
            var rawBytes = new byte[(int) this.slice.size];

            try {
                file.seek(this.slice.start);
                file.read(rawBytes, 0, (int) this.slice.size);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            var count = 0;
            while (count < this.slice.size) {
                var c = (char) rawBytes[count];
                count += 1;
                while (c != '\n') {
                    this.buffer.put((byte) c);
                    c = (char) rawBytes[count];
                    count += 1;
                }
                this.buffer.flip();
                var end = this.buffer.limit();
                var spltIndex = findSemicolon(this.buffer);

                this.buffer.limit(spltIndex);
                var station = bufferToString(this.buffer);

                this.buffer.limit(end);
                this.buffer.position(spltIndex + 1);
                var value = parseTemperature(this.buffer);

                aggr.compute(station, (_, v) -> {
                    if (v == null) {
                        return new double[]{ value, value, value, 1d };
                    }
                    v[0] = Math.min(v[0], value);
                    v[1] = Math.max(v[1], value);
                    v[2] += value;
                    v[3]++;

                    return v;
                });

                this.buffer.clear();
            }
        }

        private static int findSemicolon(ByteBuffer line) {
            int i = 0;
            while (line.get(i) != ';')
                i++;
            return i;
        }

        private static String bufferToString(ByteBuffer line) {
            byte[] bytes = new byte[line.limit()];
            line.get(0, bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private static double parseTemperature(ByteBuffer line) {
            // credit: adapted from spullara's submission
            int value = 0;
            int negative = 1;
            int i = line.position();
            while (i != line.limit()) {
                byte b = line.get(i++);
                switch (b) {
                    case '-':
                        negative = -1;
                    case '.':
                        break;
                    default:
                        value = 10 * value + (b - '0');
                }
            }
            value *= negative;
            return value / 10.0;
        }
    }

  private record ResultRow(double min, double mean, double max) {
    public String toString() {
      return round(min) + "/" + round(mean) + "/" + round(max);
    }

    private double round(double value) {
      return Math.round(value * 10.0) / 10.0;
    }
  }
}
