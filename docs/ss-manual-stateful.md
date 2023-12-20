---
layout: global
title: Stateful Stream Processing
displayTitle: Stateful Stream Processing
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

As mentioned in the Introduction to Stateless Stream Processing, records in _stateless_ streaming pipeline can be processed entirely independently of one another. To filter a record (i.e. a row), the streaming engine just needs to look at the columns of that record. However, with aggregation, the streaming engine needs to group _multiple_ records together and compute some aggregate over them; with deduplication, the streaming engine needs to remember previous records to know whether the current record on hand is a duplicate.

## Chapter Overview

Stateful stream processing is exceptionally powerful, but to get the most out of it, you'll need a little bit of streaming "theory" under your belt. (Don't worry, there aren't any proofs, at least in this section.)

So, the first three sub-sections of this Introduction to Stateful Stream Processing will give you domain knowledge in stateful stream processing. The last four sub-sections will apply this domain knowledge to Structured Streaming, and we'll show you end-to-end examples with runnable code.
