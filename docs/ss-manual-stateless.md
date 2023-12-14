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

## Projecting

Projection is the operator that takes a subset of columns from an incoming stream. Often-times upstream data streams have _tons_ of columns that your particular streaming job might not want to send downstream. Suppose your task is to create a privacy-compliant downstream table from a stream of all the new users signing up for your platform. The user stream might have:

- First and last name
- Birthday
- Home address
- Government ID

You might want to only make sure the downstream table has the name and birthday columns. To do this, you should use the `select` operator, which works with streaming DataFrames just as it works with static DataFrames:

TODO, code example.

Yay for a unified batch and streaming API!

## Generation

But sometimes, upstream sources might not have all the information you need for your streaming job. In that case, you might need to _generate_ new columns. There are two flavors of generation:

- You generate a column based off an existing column
- You generate a column based off a "standalone" function

TODO, code example. Should use `selectExpr` and built-in Spark functions. Also

## Selecting

Selection is all about keeping certain rows that satisfy a condition that you specify. In SQL, the operator used for this is commonly known as `where`, while in programming languages, this function is usually refered to as the higher-order-function (usually abbreviated as _HOF_) named `filter`. You can use either on a DataFrame. But regardless of whether you use the SQL operator or HOF, the formula is generally the same:

- You provide a string that contains your filtering predicate
- Your filtering predicate can refer to column names as strings
- You can use unary operators like `<`, `>`, `=`, `!=`

TODO, code example.

## Selection and Projection

Now, we'll put the following four concepts together to write a fairly useful stateless stream:

- Debugging sources (TODO: argh, I wish we had the memory source)
- Column projection
- Column generation
- Column selection

TODO. Let's find a REALLY good example here.
