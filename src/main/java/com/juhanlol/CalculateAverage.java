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
import java.util.HashMap;
import java.util.TreeMap;

public class CalculateAverage {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        // min, max, sum, count
        var aggrs = new HashMap<String, double[]>(1000);

        Files.lines(Paths.get(FILE))
                .forEach(line -> {
                    var parts = line.split(";");
                    var station = parts[0];
                    var value = Double.parseDouble(parts[1]);

                    aggrs.compute(station, (_, v) -> {
                        if (v == null) {
                            return new double[]{ value, value, value, 1d };
                        }
                        v[0] = Math.min(v[0], value);
                        v[1] = Math.max(v[1], value);
                        v[2] += value;
                        v[3]++;

                        return v;
                    });
                });

        var measurements = new TreeMap<String, ResultRow>();
        for (var entry : aggrs.entrySet()) {
            var station = entry.getKey();
            var aggr = entry.getValue();

            var row = new ResultRow(aggr[0], aggr[2] / aggr[3], aggr[1]);
            measurements.put(station, row);
        }

        System.out.println(measurements);
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
