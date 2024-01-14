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

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamTest}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types.{LongType, StructType}

class MemorySourceSuite extends StreamTest {
  test("basic test") {
    // scalastyle:off
    println("spark at top of test is " + spark)

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        println("from test query started")
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        println("from test query progress")
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println("from test query terminated")
      }
    })

    val schema = new StructType().add("first", LongType).add("second", LongType)

    val source = spark.readStream.format("memory").option("name", "neil").schema(schema).load()

    val df = source.select("*")

    val query = df.writeStream.format("spy").queryName("neil").start()

    println("query id is " + query.id + " and run id is " + query.runId)

    MemorySource.addData("neil", Seq(1L, 2L))
    query.processAllAvailable()

//    println(query.lastProgress)

    MemorySource.addData("neil", Seq(1L, 3L))
    query.processAllAvailable()
//    println(query.lastProgress)
  }
}
