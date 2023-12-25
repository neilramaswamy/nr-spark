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

import org.apache.spark.sql.{RowFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.streaming.sources.MemorySourceProvider.NAME_OPTION
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MemorySourceProvider extends TableProvider with DataSourceRegister {

  private var cachedTable: Table = null

  override def shortName(): String = MemorySourceProvider.SHORT_NAME

  override def supportsExternalMetadata(): Boolean = true

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
      new MemorySourceMicroBatchStream(namespace)

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }

  override def schema(): StructType = schema
}

case class MemoryInputPartition(startOffset: LongOffset, endOffset: LongOffset)
  extends InputPartition

class MemorySourceMicroBatchStream(namespace: String) extends MicroBatchStream {

  private var committedOffset = LongOffset(-1L)

  // Probably needs to read from the MemorySource singleton
  override def latestOffset(): LongOffset = {
    MemorySource.latestOffset(namespace)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[LongOffset]
    val endOffset = end.asInstanceOf[LongOffset]

    assert(startOffset.offset <= endOffset.offset)
    Array(MemoryInputPartition(startOffset, endOffset))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val memorySourcePartition = partition.asInstanceOf[MemoryInputPartition]

      new PartitionReader[InternalRow] {
        // The current index within the partition
        var currentIndex = -1L

        var isClosed = false

        override def next(): Boolean = {
          assert(!isClosed)

          currentIndex += 1
          currentIndex < memorySourcePartition.endOffset.offset
        }

        override def get(): InternalRow = {
          MemorySource.get(namespace, currentIndex.toInt)
        }

        override def close(): Unit = {
          isClosed = true
        }
      }
  }

  override def commit(end: Offset): Unit = { committedOffset = end.asInstanceOf[LongOffset] }
  override def stop(): Unit = {}

  override def initialOffset(): Offset = LongOffset(0L)

  /**
   * Deserialize a JSON string into an Offset of the implementation-defined offset type.
   *
   * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
   */
  override def deserializeOffset(json: String): Offset =
    LongOffset(json.toLong)
}

// MemorySource is the one singleton that we expose to users through which
// they can add data to a named memory source.
//
// Internally, when a DataFrame is loaded using the memory source, we register
// its name and the associated schema with the MemorySource singleton. We do
// this so that the memory source is able to serialize the user-provided tuples
// from MemorySource.addData into InternalRows.
object MemorySource {
  private var namespaceToSchemaMap = Map[String, StructType]()
  private var namespaceToDataMap = Map[String, Array[InternalRow]]()


  def addData(namespace: String, data: Any): Unit = {
    // Why isn't the Option type inferred?
    val schema: Option[StructType] = namespaceToSchemaMap.get(namespace)

    schema match {
      case Some(struct: StructType) =>
        val toRow = ExpressionEncoder(struct).createSerializer()
        val internalRow = toRow(RowFactory.create(data)).copy()

        namespaceToDataMap += namespace ->
          (namespaceToDataMap.getOrElse(namespace, Array()) :+ internalRow)
      case None => throw new IllegalArgumentException(s"Supplied namespace $namespace " +
        s"does not exist")
    }
  }

  def latestOffset(namespace: String): LongOffset = {
    val dataArray = namespaceToDataMap.get(namespace)

    dataArray match {
      case Some(arr) => LongOffset(arr.length)
      case None => throw new IllegalStateException(s"Namespace $namespace does not exist")
    }
  }

  def get(namespace: String, index: Int): InternalRow = {
    val dataArray = namespaceToDataMap.get(namespace)

    dataArray match {
      case Some(arr) => arr(index)
      case None => throw new IllegalStateException(s"Namespace $namespace does not exist")
    }
  }

  private[sql] def registerNamespace(namespace: String, schema: StructType): Unit = {
    if (namespaceToSchemaMap.contains(namespace))  {
      throw new IllegalStateException(s"Namespace $namespace is already in-use. " +
        s"Please use another name.")
    }

    namespaceToSchemaMap += (namespace -> schema)
  }
}

object MemorySourceProvider {
  val NAME_OPTION = "name"

  val SHORT_NAME = "memory"
}
