---
layout: global
title: Execution Model
displayTitle: Execution Model
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

Structured Streaming fundamentally has a _micro-batch_ execution model: the input stream is repeatedly broken up into small chunks, and each chunk is executed as a batch job. One of the main benefits of such a model is that you can write streaming jobs with an API that's almost identical to the API you'd use for batch Spark jobs. As a developer, this is of great benefit to you: you can do all your data processing with just one framework.

## The Structured Streaming Execution Loop

Structured Streaming repeatedly does three things:

1. It reads some amount of new data from a _source_
2. It processes that new data in a distributed way
3. It writes the processed data to a _sink_

There are two axes of configuration of this execution loop. They are:

1. How often does it read from the source? It could read data every hour, or every minute! You configure this with _triggers_.
2. How much "processed data" does it write to the sink? It could write just the new output it generated, or all the output it every generated. You configure this with _output mode_.

We go into more detail of triggers and output mode later. However, reading the short sections below will give you better intuition for the micro-batch architecture. There's no need to memorize everything here—you can just check the reference (TODO) later!

## Triggers: Configuring when batches run

Structured Streaming "repeatedly" breaks up an input stream into micro-batches that it executes with the Spark SQL Engine. The way that you configure the precise definition of "repeatedly" is with _triggers_. Triggers instruct Structured Streaming when it should kick-off (or trigger!) a micro-batch. The options are as follow.

### Default Trigger

Structured Streaming will choose how much of the input stream to read, and create a micro-batch out of that data. After that micro-batch finishes, it will kick-off another micro-batch.

This mode is the best balance between throughput and latency. Because you let Spark decide how much of the input to read from your source, it can choose an optimal size to maximize throughput. And because it will start a micro-batch right after finishing the last one, no time is "wasted" between micro-batches.

### Fixed-interval Trigger

Structured Streaming will kick-off micro-batches at a user-specified interval.

This mode is useful if you don't have a latency sensitive task, but you want to incrementally process data from your source. For example, suppose that you have data in cloud storage that you want to process only once per day. While you might instinctively reach for a batch job on a cron schedule, a streaming job with a fixed-interval of one day will give you benefits such as incremental processing out-of-the-box and guaranteed exactly-once processing.

### Available-now Trigger

Structured Streaming will process all data available in the source at the time when the query is started (i.e. data that arrives during query execution will not be processed).

This mode is useful if you want to run streaming jobs on a one-off basis. One-off streaming jobs are nifty because you're still incrementally processing your data source. However, since you're not constantly running micro-batches, this mode is the most economical. However, you then have to manually trigger when to run a micro-batch, which adds to operational complexity.

## Output Mode: Configuring what is written to the sink

TODO.
