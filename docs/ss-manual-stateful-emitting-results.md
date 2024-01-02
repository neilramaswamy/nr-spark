---
layout: global
title: Emitting Results and Output Mode
displayTitle: Emitting Results and Output Mode
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

The last section motivated why we should use _event-time_ in our stateful operators. Now, we'll explore our first stateful streaming operator, the streaming aggregation, which will motivate our next exciting concept, _output mode_.

## What is a streaming aggregation?

Streaming aggregations allow you to compute functions over groups of your data—it's very similar to a `GROUP BY` in SQL, but with the added complication that we records can delayed and thus come out-of-order. Let's say that we have a stream of sales and want to compute the median sale-price per hour, in real-time. To do this, Structured Streaming needs to keep _state_ for each window, which, in this case, is effectively a dictionary from window to records. For example, let's say we make the following sales at a store:

- `\$15` at 2:45pm
- `\$10` at 2:30pm
- `\$30` at 3:30pm

In this case, Structured Streaming's state would look like:

- [2pm, 3pm): `[\$15, \$10]`
- [3pm, 4pm): `[\$30]`

Then, let's say it receives two more records:

- `\$20` at 2:15pm
- `\$25` at 3:15pm

Strucutred Streaming would then update its state to look like:

- [2pm, 3pm): `[\$15, \$10, \$20]`
- [3pm, 4pm): `[\$30, \$25]`

Clearly, we've received events out-of-order. Structured Streaming is able to deal with this by buffering records in state, based on the records' event-times. The central question is, though: if Structured Streaming is buffering records per window, when will it emit an aggregate record downstream? There are two main options:

1. It can emit an aggregate once it's reasonably confident that aggregate won't change.
2. It can emit an aggregate every time an aggregate gets updated.

There are tradeoffs to both these approaches[^1], and we explore them below.

### Emitting aggregates when they won't change

When will an aggregate not change? Let's take our example from earlier. If we have a time window that ends at 3pm, the aggregate for that window won't change once we've received all records with event-times before 3pm (this isn't a clever insight; it's just the definition). So, if the streaming engine maintains a timestamp in event-time before which it won't receive any more records, it can close all windows with end-times less than that timestamp.

Such a timestamp is called a _watermark_. We will discuss how to compute a watermark very soon, but for now, let's just assume that we have a watermark. For the sake of our example, let's say the watermark becomes 3:05pm. At that point, the engine takes the following steps:

1. We find the windows with end-time less than 3:05pm. That's our 2pm to 3pm window.
2. We compute the aggregate (median) for it, which is `MEDIAN(\$15, \$10, \$20)`. So, we emit `([2pm, 3pm] -> \$15)` by appending it to the downstream sink.
3. Finally, we clean up the state for the 2pm to 3pm window, leaving only the 3pm to 4pm window in state.

Neat! Here's the takeaway: in the streaming setting, you can _finalize_ a window and remove it from state the moment that the watermark advances past the end of that window.

### Emitting aggregates with every update

While we can wait until we know that the aggregate from a window won't change, it's also reasonable to emit an aggregate's value every time that it does change. Such behavior means that for the same window, we'll emit a record multiple times. From our example earlier, this would mean that after the first batch of records, we'd emit two records downstream:

1. [2pm, 3pm): `MEDIAN([\$15, \$10]) = \$12.5`
2. [3pm, 4pm): `MEDIAN([\$30]) = \$30`

Then, after processing the next batch, we'd emit two more records downstream:

1. [2pm, 3pm): `MEDIAN([\$15, \$10]) = \$10`
2. [3pm, 4pm): `MEDIAN([\$30]) = \$27.5`

There is a clear latency benefit here, which is that we're emitting aggregates whenever they change. However, this has more caveats than you might expect:

- The aggregate value "jitters," since every time the aggregate changes, the change is emitted.
- The downstream sink is written to more. If Structured Streaming writes every record and the downstream sink charges you per-write, you'll end up paying more.
- The downstream sink needs to support updates. It needs to have some notion of versioning or recency, so when that sink is read from, it returns the most up-to-date value. The supported sinks are here (TODO).

Finally, with this mode, we've discussed when aggregates are written to the sink (i.e. every update), but when are buffered records removed from state? Again, if we don't remove records from state, then the job will eventually run out of memory. Fortunately, if we hae a _watermark_, we can remove an aggregate from state when the streaming engine will never have to use that aggregate again, i.e. when the watermark exceeds the end of the given window.

## Output Modes

The behavior of when output from aggregations is written to the sink is configured via the streaming job's _Output Mode_ (reference).

The first mode we discussed, where the engine "emits aggregates when they won't change" by using the watermark, is called "Append mode". It's called Append mode since it appends one row to the sink whenever it knows an aggregate is done. The second mode we discussed, where the engine "emits aggregates every update", is called "Update mode". You'll see how to actually set an Output Mode when you write your first aggregation, in just one more section (TODO).

<!-- TODO: This might be better put in the reference. -->

## Choosing the Right Output Mode

The most significant factor in what Output Mode you choose is the semantics of your application. If you're running a push-notification service whose sink will send a notification for every new record, you likely want to use Append Mode, since you can't update a push notification after it has been sent. On the other hand, if you're downstream sink is powering a real-time dashboard, you could use Update mode so that your dashboard stays as fresh as possible.

Here's a summary of everything we've discussed in this section:

|                            | OutputMode.Append            | OutputMode.Update                                |
| -------------------------- | ---------------------------- | ------------------------------------------------ |
| Sink support               | All sinks                    | Subset of sinks                                  |
| # of writes to sink        | The number of aggregations   | Some constant \* the number of aggregations [^2] |
| When to emit               | After aggregate is finalized | Every update                                     |
| When state removal happens | After aggregate is finalized | After aggregate is finalized                     |

<br />

<!-- ## A Summary

When you write a streaming aggregation, you're trading off several things: latency, correctness, and number of sink writes. We summarize this below:

<br />

In addition to these two parameters (latency and number of writes), also consider how you're going to use your downstream table:

- If your sink is being used to send notifications to users, update mode is probably not ideal. You don't want to send a notification for _every_ update. You probably just want to send _one_ notification when an aggregate is finalized. See the daily digest recipe for an example (TODO).
- If your sink is being used to render a real-time analytics page, update mode is probably fine. See the TODO recipe for an example.

Latency, Correctness, and Output Mode

As mentioned earlier, the time at which the streaming engine finalizes aggregates is a trade-off between latency and correctness. And as just mentioned, watermarks configure when the streaming engine finalizes aggregates. Thus, _watermarks_ are really the fundamental "knob" of streaming engines:

- If you have a really large watermark delay, you take longer to finalize aggregates, but your aggregates are more complete.
- If you have a really small watermark delay, you finalize aggregates more quickly, but your aggregates are less complete.

As we've mentioned, when you're query is running in _append_ mode (the default Output Mode), you have to set a watermark so that Structured Streaming knows when to finalize an aggregate and append it to your sink. However, if you run your query in _update_ mode, Structured Streaming will update your sink with the aggregate's most up-to-date value, _whenever_ the aggregate changes. Such behavior allows your sink to be as-up-to-date as possible, but increase the number of writes made to your sink (if you write to your sink on _every_ aggregate update, the number of writes becomes linear with respect to the number of records, as opposed to linear with respect to the number of aggregates). We summarize this tradeoff below:

LATER CHAPTERS

## Variations on aggregation

- types of windows, session windows
- aligned vs. unaligned aggregations

## Aggregating without an event-time window

Aggregating without an event-time window is a risky way to aggregate in Structured Streaming, so we don't recommend you do anything in this section. In fact, if it doesn't make sense, just move on!

So far, we've discussed aggregating on _event-time_, i.e. putting records into windows of event-time. However, you could _theoretically_ aggregate on non event-time columns. For example, you might think to yourself, "I have a stream of user-events, so I'll just count how many events for each User ID." Then, you write out `df.groupBy("userId").count()` thinking that all is well. This code has a subtle issue.

When you aggregate on a non event-time window, at no point can you (or Structured Streaming) definitively say that _all_ events for a particular `userId` have been received. It's very possible that in a few moments, you get another record for a `userId` that is already in state. As a result, Structured Streaming needs to hold on to state _forever_. With enough records, your query will use more memory than it has available, leading to an OOM—downtime for your pipeline and a headache for you.

You don't want that. So, in the streaming world, always think about aggregating with event-time windows. Instead of saying, "I want to count the number of records for each User ID," say that you want to compute the number of records for each User ID, _per day_, or _per hour_.

If you'd like to ignore this advice, keep in mind that aggregating on non event-time columns limits you to `OutputMode.Update` and `OutputMode.Complete`. Without finalization, aggregates might change after they've been emitted, and those output modes are the only ones that support changing an aggregate in a sink in-place. As such, `OutputMode.Append` is not supported. -->

[^1]: For readers with other streaming engine experience, you'll know that these aren't the _only_ two options. You could, for example, emit updated aggregates every minute, or when an aggregate changes `k` times. Those aren't yet supported, so we don't discuss them here.
[^2]: Structured Streaming will emit aggregations that have updated at the end of every micro-batch. So, if two aggregations are updated across `k` micro-batches, then `2k` writes to the sink will be made.
