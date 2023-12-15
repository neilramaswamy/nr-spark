---
layout: global
title: Event-Time, Watermarks, and Late Data
displayTitle: Event-Time, Watermarks, and Late Data
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

Consider the following situation. A customer is billed for usage of a cloud-service. They use the service at 11:50pm on a Tuesday, but the log that reports this usage reaches the billing streaming job 15 minutes late, at 12:05am on Wednesday. The streaming engine now has two choices:

- Since the event was _generated_ on Tuesday, should it put this record into the aggregate for Tuesday?
- Since it received the record on Wednesday, should it put it into the aggregate for Wednesday?

If you were the customer, what would you want? You probably would want to see your bill reflect your _actual_ usage, and not reflect your usage after the network delayed the log events you generated. You'd want the streaming engine to aggregate based on when you used the service, not when the it received your data.

## Event-Time vs. Processing-Time

The situation above highlights one of the main differences between batch and stream processing. In the streaming world, records aren't just sitting around in a table—they're constantly coming in, and any record could be delayed. As a result, in the streaming world, we have two time-domains:

- _Event-time_, which is when events are generated (e.g. by end-users)
- _Processing-time_, which is when an event is received by the streaming engine

Almost always, stateful operators will use _event-time_ in their logic. To illustrate, consider the aggregation operator, which breaks the event-time domain into windows. Upon receiving a record, the aggregation operator will place the record into the window that corresponds with that record's event-time.

In our example, the event-time of the log was 11:50pm on Tuesday, and the processing-time was 12:05am on Wednesday. So, at 12:05 when the record was received, you'd want Structured Streaming to place that record into the window for Tuesday. To sum it up:

- Records are generated in the _event-time_ domain
- Records are received in _processing-time_ domain
- Aggregations and other stateful operators use the _event-time_ domain in their logic

## Watermarks

By using event-time, we can make sure that we process records in a way that aligns with the underlying semantics of the query we're trying to run (i.e. charging the user based on when they used a service, not based on when the engine receives that log). But we have an issue.

If records can be delayed, how do we know when an aggregate is "finished"? Let's use our previous example, where the streaming engine is computing per-day aggregates of cloud service usage. If the streaming engine had decided to charge the user for their Tuesday usage _right at_ Wednesday at 12:01am, the streaming engine would have missed the delayed Tuesday record that came in at 12:05am. Even worse, if the user had used a service multiple times around 11:50pm and all those records were received around 12:05am, the streaming engine's aggregate would be wildly off.

So the central question is: in the face of delayed data, when can the streaming engine declare an aggregate as being complete? To do this, streaming engines maintain a useful timestamp called a _watermark_.

A watermark is a timestamp that has one definition, and one definition _only_ (we cannot stress that enough): a watermark is the timestamp (in event-time) before which the streaming engine does not expect any more records. If the streaming engine had a watermark of Wednesday at 12:15am, it would not expect any more recordw with event-time less than 12:15am. At that point, it could safely declare the amount of cloud-usage from Tuesday, since the only new records it would receive would be for event-times after 12:15am.

### Computing Watermarks

So how do you compute a watermark? It turns out, if you know the _maximum delay_ (i.e. the maximum gap between event-time and processing-time) between when events are generated and when they're received, you can compute a pretty decent watermark. Here's why.

TODO.

It won't hurt to reiterate the definition of a watermark:

> A watermark is the timestamp, in event-time, before which Structured Streaming will not process any more records.

TODO.

### The Only Acronym You Need: Watermarks are WET

We can concisely express everything in this article with the "WET" acronym. Watermarks are _WET_ because they:

1. **W**alk the line between event-times Structured Streaming will process, and event-times it won't.
2. **E**valuate periodically. Structured Streaming evaluates watermarks at the end of each micro-batch.
3. **T**rail behind the largest event-time by a fixed delay. This is how you compute a watermark.

The first bullet is the definition of the watermark. The second and third bullets are technically implementation details, but they'll help you fine-tune your pipelines for certain cases (i.e. backfills) and latency. We'll talk about those later!
