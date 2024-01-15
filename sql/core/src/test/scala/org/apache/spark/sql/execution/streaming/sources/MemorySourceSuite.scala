/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.sql.streaming.{StreamTest}
import org.apache.spark.sql.types.{LongType, StructType}

class MemorySourceSuite extends StreamTest {
  test("basic test") {
    val schema = new StructType().add("first", LongType).add("second", LongType)

    val source = spark.readStream.format("memory").option("name", "neil").schema(schema).load()

    val df = source.select("*")

    val query = df.writeStream.format("spy").queryName("neil").start()

    // TODO(neil): Schemafy the resulting table, see why dups in sink.
    MemorySource.addData("neil", Seq(1L, 2L))
    query.processAllAvailable()

    MemorySource.addData("neil", Seq(1L, 3L))
    MemorySource.addData("neil", Seq(1L, 4L))
    MemorySource.addData("neil", Seq(5L, 6L))
    query.processAllAvailable()
  }
}
