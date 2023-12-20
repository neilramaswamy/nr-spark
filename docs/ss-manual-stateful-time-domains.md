---
layout: global
title: Event-Time and Processing-Time
displayTitle: Event-Time and Processing-Time
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

One very common use-case of Structured Streaming is to implement billing pipelines. Billing pipelines aggregate usage logs per-customer, per-day (or per-month) and compute some function over those logs to determine how much money to charge a customer. A very simple pricing model could be $0.01 per log, and the aggregation logic would just have to count the number of records per-customer per-day, and multiply by 1 cent.

## Batch Aggregations vs. Streaming Aggregations

In the batch setting, this actually is fairly straightforward. In "batch" SQL, this is a `COUNT(LogId) * 0.01` over a `GROUP BY customerId, day`. In the streaming setting, on the other hand, we're receiving records in real-time: there's no static table sitting around anywhere. As a result, the streaming engine needs to deal with _two_ time domains: when a record was generated, and when that record was received by the system. Understanding this distiction is crucial for guaranteeing the correctness of streaming queries.

Consider the following situation. A customer uses a cloud service at 11:50pm on a Tuesday, but the log that reports this usage reaches the billing streaming job 15 minutes late, at 12:05am on Wednesday. The streaming engine now has two choices:

- Since the event was _generated_ on Tuesday, should it put this record into the aggregate for Tuesday?
- Since it received the record on Wednesday, should it put it into the aggregate for Wednesday?

If you were the customer, what would you want? You probably would want to see your bill reflect your _actual_ usage, and not reflect your usage after the network delayed the log events you generated. You'd want the streaming engine to aggregate based on when you used the service, not when the it received your data. We need to make sure that the streaming engine knows to do this.

## Event-Time vs. Processing-Time

In the streaming world, the two time domains have more formal names:

- _Event-time_ is the time-domain in which events are generated (e.g. by end-users)
- _Processing-time_ is the time-domain in which events are received by servers, i.e. Kafka, Kinesis, etc.

Almost always, stateful operators should use _event-time_ in their logic. With aggregation for example, the aggregation operator should place the record in the window corresponding to the event-time extracted from the record itself. Very shortly, we'll show you the syntax for specifying the name of a record's event-time column to Structured Streaming, but for now, you just need to remember that these two time-domains exist.
