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

Very frequently, data sources have duplicate records. This mostly happens because many data systems only have at-least-once guarantees, so that same record can get delivered multiple times. For example, a first-tier server might be trying to send a record to a database cluster; if the first-tier server has a retry mechanism that isn't idempotent, it could produce that record many times. This happens very frequently.

As a result, deduplication of streams is needed. You can deduplicate on one or more columns, and you can simply pass the columns on which you want to deduplicate to a deduplication function. There are two such deduplication functions:

- `dropDuplicatesWithinWatermark`: this function will hold onto duplicates for at-least as much time as the watermark duration in your job. As a result, it will perform state removal (using the watermark), which is good for scalability.
- `dropDuplicates`: this function should be used when you want _global_ deduplication across your entire stream. Beware! Global deduplication requires unbounded amounts of memory, so unless you're sure your stream is low-cardinality, don't use this!

## The scalable option: `dropDuplicatesWithinWatermark`

TODO.

## The dangerous option: `dropDuplicates`

TODO.
