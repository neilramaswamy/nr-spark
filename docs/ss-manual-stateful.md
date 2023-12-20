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

As a result, to implement a stateful query like an aggregation or deduplication, Structured Streaming needs to buffer rows in memory. When there are very few rows, such buffering is as simple as keeping the records in a hash map. When there are more rows (some users have deduplication jobs running at several millions of records per second!), it relies on RocksDB to efficiently spill keys onto the local disk.
