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

1. It reads some amount of _new_ data from a source
2. It processes that new data by executing a _micro-batch_
3. It writes the processed data to a _sink_

We'll investigate all of these three things in more detail, but a relevant question now is, "what does it mean to 'repeatedly' do these things?" Should the streaming engine run a micro-batch every second, every minute, or every hour; or should it just run a micro-batch whenever new data is available.

## Triggers: Configuring when batches run

Structured Streaming "repeatedly" breaks up an input stream into micro-batches that it executes with the Spark SQL Engine. The way that you configure the precise definition of "repeatedly" is with _triggers_. Triggers instruct Structured Streaming when it should kick-off (or trigger!) a micro-batch. The options are as follow.

- **Default trigger**: Structured Streaming runs micro-batches back-to-back. After finishing one micro-batch, if there's new data in the source, it will start another micro-batch to process that new data.
- **Fixed-interval trigger**: Structured Streaming will run micro-batches at an interval you specify.
- **Available-now trigger**: Structured Streaming will process all data that is available when the streaming job begins. After processing that data, the streaming job will exit.

One of the main differences between a streaming and batch job is that streaming jobs _incrementally_ processes their source—they don't have to read the entire source every time they start. The fixed-interval and available-now triggers allow you to incrementally process your source, without having to constantly run the job.

Just a heads-up: in many of the examples in this manual, we'll use the available-now trigger so that we can run one micro-batch, process all the new data in some testing data source, and then inspect the output.

## Output Mode: Configuring what is written to the sink

TODO(wei). Let's briefly discuss the 3 output modes, emphasizing _why_ you might want to use a certain one. The reference (which doesn't exist yet) can have all the in-the-weeds details about which output mode doesn't work with xyz stateful operator. But we should give practical advice here, like, "doing outputmode update means that we update the downstream table in place, which can get expensive if you're charged per-write." stuff like that.
