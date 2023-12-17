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

## What is a streaming aggregation?

Streaming aggregations allow you to compute statistics over groups of your data—it's very similar to a `GROUP BY` in SQL, but with the added complication that records can come out-of-order. Let's say that we have a stream of sales and want to compute the _median_ sale-price per hour. To do this, Structured Streaming needs to keep _state_ for each window. For example, let's say we make the following sales at a store:

- `$10` at 2:30pm
- `$20` at 2:15pm
- `$15` at 2:45pm
- `$30` at 3:30pm

In this case, Structured Streaming's _state_ would look like:

- [2pm, 3pm): `[$10, $20, $15]`
- [3pm, 4pm): `[$30]`

But note: at this point, Structured Streaming hasn't emitted anything about the median during 2pm-3pm to the sink. In fact, even though the current time at the store might be 3:45pm, we haven't emitted the data from 2pm-3pm to the sink. While we know that streaming engines need to wait some time due to late-data, when _precisely_ can they finalize an aggregate and emit it?

## Closing aggregates and cleaning up state

Here's where watermarks come in: if we have a timestamp before which we don't expect to receive any more records (the watermark), we can finalize aggregates before that point. In particular, we do three things when the watermark advances:

1. Find all windows with an end that is less than the new watermark
2. Compute the aggregate function, and emit the result downstream
3. Clean up the state associated with that window

So, in our example, supposes the watermark advances just past 3pm:

1. The only open window with end time less than 3pm is the 2pm - 3pm window.
2. We compute the aggregate (median) for it, which is `MEDIAN($10, $20, $15)`. So, we emit $15 downstream.
3. Finally, we clean up the state for the 2pm to 3pm window, leaving only the 3pm - 4pm window in state.

Neat! Here's the takeaway: in the streaming setting, the moment that you _finalize_ an aggregate is the moment that the watermark advances past the end of a window. Thus, you always want to define a watermark when implementing streaming aggregations.

## Variations on aggregation

- types of windows, session windows
- aligned vs. unaligned aggregations

## Latency, Correctness, and Output Mode

It actually turns out that your choice of watermark is _really_ important for both the latency and correctness of your pipeline:

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
