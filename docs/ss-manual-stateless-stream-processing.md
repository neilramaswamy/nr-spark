---
layout: global
title: Stateless Stream Processing
displayTitle: Stateless Stream Processing
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

Stateless stream processing is the first type of stream processing we'll explore. It's characterized by a very simple property: every record can be processed _fully_ independently of every other record. For example:

- Selection is stateless, since you just have to check if a single given record passes the filter predicate
- Projecting is stateless, since taking a subset of a record's columns can be done without looking at any other records

We use the word "stateless" because this nice property means that Structured Streaming doesn't need to have any "memory" (i.e. state) about what it has seen. It can just filter a record, or take some columns from the record, and move on. Aggregation, on the other hand, is stateful, since Structured Streaming needs to remember (i.e. keep state) for all the records that belong in an aggregate.

## Selecting

TODO. Should mention syntax (both SQL and HOF).

## Projecting

TODO. Should mention syntax (both SQL and HOF) for:

- Taking a subset of columns
- Adding a new column with a Spark fn

## Selection and Projection

TODO. Let's give an E2E example here, maybe with rate source. Could be something like, just selecting the value column, and only when value % 10 is 0, and adding another column with a Spark built-in function. Just need to showcase everything.
