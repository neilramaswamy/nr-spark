---
layout: global
title: Reference
displayTitle: Reference
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Default Trigger

Structured Streaming will choose how much of the input stream to read, and create a micro-batch out of that data. After that micro-batch finishes, it will kick-off another micro-batch.

This mode is the best balance between throughput and latency. Because you let Spark decide how much of the input to read from your source, it can choose an optimal size to maximize throughput. And because it will start a micro-batch right after finishing the last one, no time is "wasted" between micro-batches.

### Fixed-interval Trigger

Structured Streaming will kick-off micro-batches at a user-specified interval.

This mode is useful if you don't have a latency sensitive task, but you want to incrementally process data from your source. For example, suppose that you have data in cloud storage that you want to process only once per day. While you might instinctively reach for a batch job on a cron schedule, a streaming job with a fixed-interval of one day will give you benefits such as incremental processing out-of-the-box and guaranteed exactly-once processing.

### Available-now Trigger

Structured Streaming will process all data available in the source at the time when the query is started (i.e. data that arrives during query execution will not be processed).

This mode is useful if you want to run streaming jobs on a one-off basis. One-off streaming jobs are nifty because you're still incrementally processing your data source. However, since you're not constantly running micro-batches, this mode is the most economical. However, you then have to manually trigger when to run a micro-batch, which adds to operational complexity.
