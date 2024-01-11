#!/bin/sh
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Setup
# 2g mem, 4 cores

# Results
# baseline: 3m02s
# copy chunk 5k lines, 4 threads: 1m09s


JAVA_OPTS="--enable-preview -Xmx2g -Xms2g -XX:+AlwaysPreTouch -XX:ActiveProcessorCount=4"
# time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar com.juhanlol.CalculateAverage
time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar com.juhanlol.CalculateAverageSlices
