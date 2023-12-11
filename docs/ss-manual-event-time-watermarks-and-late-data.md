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

Almost always, we want to aggregate based on _event-time_. In our example, the event-time of the log was 11:50pm, and the processing-time was 12:05am. If we aggregate on event-time, we won't overcharge the user, since the log will be put into the window for Tuesday. We'll shortly see how to tell Structured Streaming what column of our source records it should use to find a record's event-time.

## Watermarks

By aggregating on event-time, we can deal with delayed data. But we actually have a problem on our hands: if events can be delayed, how can we ever know for sure that an aggregate is "done"? Consider the following: the customer used a cloud credit at 11:30pm, but it's an hour delayed. If the streaming engine didn't wait for this delayed record, it would under-report the user's usage!

So the central question is: in the face of delayed data, how can the streaming engine know when an aggregate is complete? To do this, we're going to assume the existence of a magical timestamp called a _watermark_.

A watermark is a timestamp that says one thing, and one thing _only_ (we cannot stress that enough): a watermark is the timestamp before which the streaming engine will not receive any more records. If the streaming engine had a watermark of Wednesday at 12:05am, it would not receive any records before Wednesday at 12:05am. Thus, it can be sure that no more records for Tuesday will be received, so it can go ahead and tell a downstream service to bill users for Tuesday.

### Computing Watermarks

TODO. And diagram.
