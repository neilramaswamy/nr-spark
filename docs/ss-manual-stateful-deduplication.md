---
layout: global
title: Deduplication
displayTitle: Deduplication
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

Very frequently, data sources have duplicate records. This mostly happens because many data systems only have at-least-once guarantees, so that same record can get delivered multiple times. For example, a web server might be trying to send a write to a database cluster; if the web server has a retry mechanism that isn't idempotent, it could produce that record many times. In that case, those records records would be duplicated!

As a result, deduplication of streams is needed. You can deduplicate on one or more columns, and you can simply pass the columns on which you want to deduplicate to a deduplication function. There are two such deduplication functions:

- `dropDuplicatesWithinWatermark`: this function will hold onto duplicates for at-least as much time as the watermark duration in your job. As a result, it will perform state removal (using the watermark), which is good for scalability.
- `dropDuplicates`: this function should be used when you want _global_ deduplication across your entire stream. Beware! Global deduplication requires unbounded amounts of memory, so unless you're sure your stream is low-cardinality, don't use this!

## The scalable option: `dropDuplicatesWithinWatermark`

This is the deduplication operator you should reach for first. Most of the time with duplication, you know ahead of time the interval of time within which you might receive duplicates (TODO: is this true? what do people use to figure this out?). If you know that you'll have duplicates within a `k` minute interval, you can instruct Structured Streaming to hold onto records for `k` minutes (so that it can perform deduplication), and then purge the records that have been around for more than `k` minutes.

For example, suppose we know that we'll have duplicates within 5 minutes of each other. If we receive a record, `ID = foo`, with event-time 6, we might receive duplicates up to event-time 5+6 = 11. So, take a moment to think: what tool tells us when we're out of the "danger" zone of receiving an event-time before 11?

Watermarks do that! If duplicates for that record can arrive up until 11, once we know that we'll no longer receive event-times _before_ 11, we can purge that record. That's where the name `dropDuplicatesWithinWatermark` comes from: for at least as many units of time as your watermark delay that you give (i.e. _within_ your watermark delay), Structured Streaming will perform deduplication. So, always use `withWatermark` if you're using `dropDuplicatesWithinWatermark`.

Let's do an example. TODO.

That being said, if you only drop duplicates _within_ your watermark, you might have duplicates _beyond_ your watermark (in our example, another record with `ID = foo` that arrives after event-time 11 will likely not be deduplicated). If you have strict deduplication requirements, you have two options:

1. Keep state for longer via a larger watermark delay (a larger watermark means more records arrive _within_ your watermark)
2. Keep state forever, using `dropDuplicates`, which is explained below

## The dangerous option: `dropDuplicates`

This function has a special power, but it must be wielded with care: `dropDuplicates` allows you deduplicate over _all_ records of the stream. Since streams are unbounded, this means that `dropDuplicates` can deduplicate over an unbounded number of records, by keeping all the records it sees in state. This behavior has an upside and a downside:

1. Upside: you get perfect, total deduplication!
2. Downside: it has to store every record in state. If you have enough records, you'll run out of space to store records. Your machines will then crash, due to Out-of-Memory errors.

Here is one last warning:

> For this reason of unbounded state growth, `dropDuplicates` is one operator that does not cleanly transfer over from the batch APIs to the streaming APIs. If you have a batch job with `dropDuplicates` and you want to migrate it to a streaming job, you really should really change your job to use `dropDuplicatesWithinWatermark`.

With that out of the way, here's the syntax for it. You just pass it the columns, as strings, on which you want to deduplicate:

TODO.

(Small aside for users from previous Spark versions: in the past, we used to suggest deduplication with state removal by passing an event-time column to `dropDuplicates`. This still works, but we highly recommend using `dropDuplicatesWithinWatermark` instead—it's less error-prone.)

## An End-to-End Deduplication Example

Note: for all E2E examples we give, we need CLEAR separation (i.e. provide comments) between:

- source creation
- operators
- sink writing

## Summary
