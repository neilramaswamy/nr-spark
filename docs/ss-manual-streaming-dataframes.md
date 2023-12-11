---
layout: global
title: Creating Streaming DataFrames
displayTitle: Creating Streaming DataFrames
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

To do processing over streams of data, we need to create a _streaming DataFrame_. A streaming DataFrame is effectively a static DataFrame, in that the Spark APIs still apply—the only difference is that a streaming DataFrame has extra magic (that you never have to worry about!) that incrementally reads its data source.

The `SparkSession.readStream` API returns a `DataStreamReader` ([Python](api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader)/[Scala](api/scala/org/apache/spark/sql/streaming/DataStreamReader.html)/[Java](api/java/org/apache/spark/sql/streaming/DataStreamReader.html) docs) which exposes several useful functions for configuring (and eventually returning) a streaming DataFrame. (Note: in [R](api/R/reference/read.stream.html), you need to use the `read.stream()` method). The most relevant functions are as follow:

- `DataStreamReader.format`, which lets you configure what _type_ of source you want to read from (e.g. Cloud storage, Kafka, etc.)
- `DataStreamReader.option`, which lets you pass options to the source of your choosing (e.g. authentication options, resource name)
- `DataStreamReader.load`, which takes no arguments, and just returns a _streaming_ DataFrame

We'll use these 3 methods down below. But once you call `.load()` and you have a streaming DataFrame, you can start transforming it with the Spark APIs—we'll get to that in the next section.

## An Overview of Built-In Sources

Structured Streaming understands several source formats out-of-the-box. We defer an extensive discussion of each format and their configuration options to the reference (TODO). However, here is a brief overview of production sources:

- **File source** - Reads files written in a directory as a stream of data. Files will be processed in the order of file modification time. Supported file formats are text, CSV, JSON, ORC, Parquet. This mode enables you to keep your structured data in cloud storage, which will be cheaper for you than storing your data in, say, a relational database. See the TODO for more details.
- **Kafka source** - Reads data from Kafka. It's compatible with Kafka broker versions 0.10.0 or higher. See the [Kafka Integration Guide](structured-streaming-kafka-integration.html) for more details.

Structured Streaming also supports several testing-only modes. They are to never be used in production:

- **Rate source (for testing)** - Generates data at the specified number of rows _per second_. Each output row contains a `timestamp` and `value`. This source is useful when load-testing your jobs, since it allows you to easily generate 1000s of rows per second. See TODO for more details.
- **Rate Per Micro-Batch source (for testing)** - Generates data at the specified number of rows _per micro-batch_. Each output row contains a `timestamp` and `value`. Unlike the rate data source, this data source provides a consistent set of input rows per _micro-batch_ (not per second). See TODO for more details.
- **Socket source (for testing)** - Reads UTF8 text data from a socket connection. See TODO for more details.

## Advice: Using Development Sinks When Learning Structured Streaming

TODO. Here, we give them advice about how to use the rate (or some other) source for testing their pipelines or the behavior of SS. Then, give them an example with that source so that they get a feel for using the `format`, `option`, and `load` methods.
