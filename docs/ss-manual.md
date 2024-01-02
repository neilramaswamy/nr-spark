---
layout: global
title: Structured Streaming Manual
displayTitle: Structured Streaming Manual
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

## Overview

Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the [Dataset/DataFrame API](sql-programming-guide.html) in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc.

## Why streaming?

Spark revolutionized the way that organizations interacted with their data: with Spark, businesses were able to extract insights from the vast amount of data that they had, faster than ever before. However, the world very quickly turned real-time, with businesses expecting insights based on _current_ data, and users expecting features to reflect the now. Structured Streaming unlocks these real-time use cases, all within the same familiar, powerful, and mature Spark ecosystem.

## Why Structured Streaming?

Structured Streaming executes repeatedly executes small "micro-batches" over the source stream by using the Spark SQL engine, which has benefitted from over a decade of continuous optimization. Structured Streaming can scale to processing petabytes of data per day, all while ensuring end-to-end exactly-once fault-tolerance guarantees. In addition to high throughput (great for ETL!), it provides latencies low enough for operational workloads; it supports a low-latency processing mode called Continuous Mode that can achieve end-to-end latencies as low as 100ms.

In this guide, we'll walk you through the programming model and the APIs. We'll to explain the concepts mostly using the default micro-batch processing model, and then [later](#continuous-processing) discuss the Continuous Processing model.
