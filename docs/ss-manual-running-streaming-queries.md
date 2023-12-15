---
layout: global
title: Running Streaming Queries
displayTitle: Running Streaming Queries
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

Let's recap how far we've come:

- We talked about the microbatch [execution model](./ss-manual-execution-model.md), including output modes and triggers
- We discussed [creating sources](./ss-manual-streaming-dataframes.html)
- We then covered [stateless](./ss-manual-stateless.html) and [stateful](./ss-manual-stateful.html) operators

There are just a few things left to cover before you're all set:

- Configuring a sink to write the results of your operators
- Ensuring fault-tolerance through a _checkpoint location_
- Specifying the _trigger_ and _output mode_

## The `writeStream` method

Every DataFrame exposes the `.writeStream` method, which gives you back an interface that exposes the following methods:

- `format`, which consumes a string of the sink you'd like to write to. See all sinks here (TODO).
- `option`, which lets you set a key-value to configure your sink and query

- `trigger`, which consumes the `Trigger` you want your query to use
- `outputMode`, which consumes the `OutputMode` you want your query to use

At this point, we've discussed almost all the operators\_ supported by Structured Streaming. There are now just three last things between you and running your first, real-life query:

This section should have:

- The supported sinks
- Recap of output modes/triggers
- Checkpoint location
