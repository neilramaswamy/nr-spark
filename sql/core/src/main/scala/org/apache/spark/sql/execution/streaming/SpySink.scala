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

package org.apache.spark.sql.execution.streaming

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.sources.PackedRowWriterFactory
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class SpyRelation(override val sqlContext: SQLContext)
  extends BaseRelation {
  override def schema: StructType = StructType(Nil)
}

class SpySinkProvider extends SimpleTableProvider
  with DataSourceRegister
  with CreatableRelationProvider {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SpyTable
  }

  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    SpyRelation(sqlContext)
  }

  def shortName(): String = "spy"
}

class SpyTable() extends Table with SupportsWrite {
  override def name(): String = "spy"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.STREAMING_WRITE)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {
      private val inputSchema: StructType = info.schema()

      // Do nothing for truncate. Spy sink is special and it just prints all the records.
      override def truncate(): WriteBuilder = this

      override def build(): Write = {
        new Write {
          override def toStreaming: StreamingWrite = {
            assert(inputSchema != null)
            new SpyWriter()
          }
        }
      }
    }
  }
}

// SpyWriter is initialized with a reference to an array of messages
// owned by its caller Table. This is because the driver has access
// only to the Table, and it needs to be able to access the messages
// so that it can print them when the StreamingQueryProgress becomes
// available.
private class SpyWriter()
  extends StreamingWrite with Logging {
  assert(SparkSession.getActiveSession.isDefined)

  // Register a streaming query listener
  val spark = SparkSession.getDefaultSession.get

//  SpyWriter.initializeListener(spark)
  // scalastyle:off println
  println(s"Adding listener for current spark session ${spark}")

  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      println("From Spy query started!")
    }
    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      println(s"From Spy query made progress: ${event.progress}")
    }
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      println("From Spy query terminated!")
    }
  })

  override def useCommitCoordinator(): Boolean = false

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // scalastyle:off println
    println(s"Called commit for epoch $epochId with num messages ${messages.length}")
  }

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def toString(): String = {
    s"SpyWriter"
  }

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo)
      : StreamingDataWriterFactory = PackedRowWriterFactory
}

object SpyWriter {
  println("SpyWriter object created!")
}

