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

import java.util
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.streaming.continuous.RateStreamContinuousStream
import org.apache.spark.sql.execution.streaming.sources.MemorySourceProvider.NAME_OPTION
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MemorySourceProvider extends TableProvider with DataSourceRegister {

  private var cachedTable: Table = null

  override def shortName(): String = "memory"

  // This is called when a user-specific schema is not present. However, a memory source
  // can't infer its schema, since it's entirely up to the user. As a result, if inferSchema
  // is called before getTable, we have to throw an error.
  //
  // Ideally, we should add an interface like RequiresSchema, but we can do that later.
  override def inferSchema(options: CaseInsensitiveStringMap)
      : StructType = {
    if (cachedTable == null) {
      throw new IllegalStateException("Schema must be provided to memory source: " +
        "it cannot be inferred.")
    }

    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    cachedTable.columns().asSchema
  }

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String])
      : Table = {
    // Create a MemorySourceTable with the given schema and name
    val namespace = properties.get(NAME_OPTION)
    if (namespace == null) {
      throw new IllegalArgumentException(s"Table name is not specified in options.")
    }

    cachedTable = new MemorySourceTable(namespace, schema)
    cachedTable
  }
}

class MemorySourceTable(namespace: String, schema: StructType)
  extends Table with SupportsRead {

  override def name(): String = {
    s"MemorySource(namespace=$namespace)"
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new MemorySourceMicroBatchStream(namespace, schema)

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }
}

class MemorySourceMicroBatchStream(namespace: String, schema: StructType) extends MicroBatchStream {
  override def latestOffset(): LongOffset = {
    MemorySourceOffset(Map(str -> 0L))
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[MemorySourceOffset].partitionToValue(str)
    val endOffset = end.asInstanceOf[MemorySourceOffset].partitionToValue(str)
    assert(startOffset <= endOffset)
    Array(MemoryInputPartition(startOffset, endOffset))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    MemoryPartitionReaderFactory
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
}




object MemorySourceProvider {
  val NAME_OPTION = "name"
}
