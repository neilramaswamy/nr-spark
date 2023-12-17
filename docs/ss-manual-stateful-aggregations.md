---
layout: global
title: Aggregations
displayTitle: Aggregations
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

Our first type of stateful query is the _aggregation_. As discussed in the introduction to stateful operators, there are two things that are top-of-mind for us:

- If events can come out-of-order, how do we know when an aggregate is complete?
- If we have to keep state for our aggregates, how do we make sure state doesn't grow to be infinitely large?

## Closing aggregates and cleaning up state

TODO.

## Aggregation

- Aggregating on non-event time
- Aggregating on event-time

## Latency, Correctness, and Output Mode

From this discussion, we see that the watermark delay that you set fundamentally becomes a tradeoff between latency and correctness:

- If you have a really large watermark delay, you take longer to finalize aggregates, but your aggregates are more complete.
- If you have a really small watermark delay, you finalize aggregates more quickly, but your aggregates are less complete.

As we've mentioned, when you're query is running in _append_ mode (the default Output Mode), you have to set a watermark so that Structured Streaming knows when to finalize an aggregate and append it to your sink. However, if you run your query in _update_ mode, Structured Streaming will update your sink with the aggregate's most up-to-date value, _whenever_ the aggregate changes. Such behavior allows your sink to be as-up-to-date as possible, but increase the number of writes made to your sink (if you write to your sink on _every_ aggregate update, the number of writes becomes linear with respect to the number of records, as opposed to linear with respect to the number of aggregates). We summarize this tradeoff below:

|                     | OutputMode.Append              | OutputMode.Update                   |
| ------------------- | ------------------------------ | ----------------------------------- |
| Latency             | Depends on watermark           | Always low                          |
| # of writes to sink | Linear w.r.t number of records | Linear w.r.t number of aggregations |

<br />
In addition to these two parameters (latency and number of writes), also consider how you're going to use your downstream table:

- If your sink is being used to send notifications to users, update mode is probably not ideal. You don't want to send a notification for _every_ update. You probably just want to send _one_ notification when an aggregate is finalized. See the daily digest recipe for an example (TODO).
- If your sink is being used to render a real-time analytics page, update mode is probably fine. See the TODO recipe for an example.

## Aggregating without an event-time window

Aggregating without an event-time window is a risky way to aggregate in Structured Streaming, so we don't recommend you do anything in this section. In fact, if it doesn't make sense, just move on!

So far, we've discussed aggregating on _event-time_, i.e. putting records into windows of event-time. However, you could _theoretically_ aggregate on non event-time columns. For example, you might think to yourself, "I have a stream of user-events, so I'll just count how many events for each User ID." Then, you write out `df.groupBy("userId").count()` thinking that all is well. This code has a subtle issue.

When you aggregate on a non event-time window, at no point can you (or Structured Streaming) definitively say that _all_ events for a particular `userId` have been received. It's very possible that in a few moments, you get another record for a `userId` that is already in state. As a result, Structured Streaming needs to hold on to state _forever_. With enough records, your query will use more memory than it has available, leading to an OOM—downtime for your pipeline and a headache for you.

You don't want that. So, in the streaming world, always think about aggregating with event-time windows. Instead of saying, "I want to count the number of records for each User ID," say that you want to compute the number of records for each User ID, _per day_, or _per hour_.

If you'd like to ignore this advice, keep in mind that aggregating on non event-time columns limits you to `OutputMode.Update` and `OutputMode.Complete`. Without finalization, aggregates might change after they've been emitted, and those output modes are the only ones that support changing an aggregate in a sink in-place. As such, `OutputMode.Append` is not supported.
