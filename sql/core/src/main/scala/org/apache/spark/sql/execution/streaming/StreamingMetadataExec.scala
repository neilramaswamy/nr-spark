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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, JoinedRow}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class StreamingMetadataExec(child: SparkPlan, output: Seq[Attribute]) extends UnaryExecNode {

  lazy val metadataRow = {
    // Create a projection that will include metadata columns, if they exist
    val metadataColumns = output.filter(_.metadata.contains("_streaming_metadata"))

    val metadataContent: Array[Any] = metadataColumns.map {
      case _ => 420
    }.toArray
    new GenericInternalRow(metadataContent)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      iter.map { row =>
        new JoinedRow(row, metadataRow)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StreamingMetadataExec = {
    copy(child = newChild)
  }
}
