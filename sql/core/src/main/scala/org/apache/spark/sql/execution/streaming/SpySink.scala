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

import scala.collection.immutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.sources.{PackedRowCommitMessage, PackedRowWriterFactory}
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SingleColumnConsoleTableBuilder


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
            new SpyWriter(info.queryId())
          }
        }
      }
    }
  }
}

// SpyWriter is a streaming writer that writer that spies on the query progress
// and prints it out along with the written data.
private class SpyWriter(queryId: String)
  extends StreamingWrite with Logging {
  assert(SparkSession.getActiveSession.isDefined)

  // TODO(neil): Figure out why the activeSession doesn't work
  val spark = SparkSession.getDefaultSession.get

  SpyWriter.registerListener(spark, queryId)

  override def useCommitCoordinator(): Boolean = false

  // Commit is called _before_ the streaming query makes progress, which happens at
  // the end of the batch. As a result, we need to store the messages for this epochId
  // until the associated streaming query progress comes along, at which point we
  // print (and evict) those messages.
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // scalastyle:off
    println(s"Commiting for epoch ${epochId} with num messages ${messages.length}")

    val rows = messages.collect {
      case PackedRowCommitMessage(rs) => rs
    }.flatten

    // Print out every row
    rows.foreach(println)

    SpyWriter.registerMessages(queryId, epochId, rows)
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
  // Keeps track of all the queryIds that currently have Spy listeners.
  // TODO(neil): Check why SpyWriter seems to be initialized multiple times
  //  per batch.
  private val activeQueryIds = scala.collection.mutable.Set[String]()

  // Store the messages for each batch, for each query (the data from each batch is
  // copied here during SpyTable::commit. We store the mesages per batch so that
  // we can print them out when we receive the streaming query progress.
  private val messagesPerEpoch = scala.collection.mutable.Map[String,
    scala.collection.mutable.Map[Long, Array[InternalRow]]]()

  private def registerListener(spark: SparkSession, queryId: String): Unit = {
    var listenerExists = false

    activeQueryIds.synchronized {
      if (activeQueryIds.contains(queryId)) {
        listenerExists = true
      } else {
        activeQueryIds += queryId
      }
    }

    if (listenerExists) {
      return
    }

    // scalastyle:off println
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("From Spy query started!")
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println(s"[ON PROGRESS] For queryId ${queryId} and batchId ${event.progress.batchId}")

        val messages = messagesPerEpoch.synchronized {
          messagesPerEpoch(queryId)(event.progress.batchId)
        }

        val batchId = event.progress.batchId
        var table = new SingleColumnConsoleTableBuilder(s"Batch ${batchId} Progress")

        table = table
          .addHeader("WRITES TO SINK")
          .addRows(immutable.ArraySeq.unsafeWrapArray(messages.map(_.toString)): _*)

        // Only print the watermark if there is one
        if (event.progress.eventTime.size() != 0) {
          table = table
            .addHeader("WATERMARK")
            .addRows(s"value -> ${event.progress.eventTime.get("watermark")}")
            .addRows(s"numRowsDroppedByWatermark -> " +
              s"${event.progress.stateOperators.map(_.numRowsDroppedByWatermark).sum}")
        }

        // Print the number of rows in state
        if (event.progress.stateOperators.length != 0) {
          table = table
            .addHeader("STATE")
            .addRows(s"numRowsTotal -> " +
              s"${event.progress.stateOperators.map(_.numRowsTotal).sum}")
        }

        table.print()

        messagesPerEpoch.synchronized {
          messagesPerEpoch(queryId).remove(event.progress.batchId)
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("From Spy query terminated!")
        activeQueryIds.synchronized {
            activeQueryIds -= queryId
        }
      }
    })
  }

  private def registerMessages(
      queryId: String,
      epochId: Long,
      messages: Array[InternalRow]): Unit = {
    messagesPerEpoch.synchronized {
      val messageMap = messagesPerEpoch.getOrElseUpdate(queryId,
        scala.collection.mutable.Map[Long, Array[InternalRow]]())
      messageMap(epochId) = messages
    }
  }

  println("SpyWriter object created!")
}

