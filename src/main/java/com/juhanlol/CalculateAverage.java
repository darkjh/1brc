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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CalculateAverage {

    private static final String FILE = "./measurements.txt";
    private static final int BATCH_SIZE = 5000;
    private static final int NUM_THREADS = 4;

    public static void main(String[] args) throws IOException, InterruptedException {
        var aggrs = new ArrayList<HashMap<String, double[]>>(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            aggrs.add(new HashMap<>(1000));
        }

        var pool = Executors.newWorkStealingPool(NUM_THREADS);
        var permits = new AtomicInteger(NUM_THREADS);

        var iter = Files.lines(Paths.get(FILE)).iterator();

        while (iter.hasNext()) {
            if (permits.get() > 0) {
                var lines = new ArrayList<String>(BATCH_SIZE);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    if (iter.hasNext()) {
                        lines.add(iter.next());
                    }
                }

                permits.decrementAndGet();
                CompletableFuture.runAsync(new Task(lines, aggrs), pool)
                        .whenComplete((__, ignored) -> {
                            permits.incrementAndGet();
                        });
            }
        }

        pool.shutdown();
        pool.awaitTermination(1000000, TimeUnit.DAYS);

        var all = aggrs.get(0);
        for (int i = 1; i < NUM_THREADS; i++) {
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
        var measurements = new TreeMap<String, ResultRow>();
        for (var entry : all.entrySet()) {
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
        private final ArrayList<String> lines;
        private final ArrayList<HashMap<String, double[]>> aggrs;

        Task(ArrayList<String> lines, ArrayList<HashMap<String, double[]>> aggrs) {
            this.lines = lines;
            this.aggrs = aggrs;
        }

        @Override
        public void run() {
            var aggr = aggrs.get(ThreadId.get());

            for (var line : lines) {
                var parts = line.split(";");
                var station = parts[0];
                var value = Double.parseDouble(parts[1]);

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
            }
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
