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

TODO.
