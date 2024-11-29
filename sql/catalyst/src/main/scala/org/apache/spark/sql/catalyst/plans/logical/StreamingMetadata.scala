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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.trees.TreePattern.{STREAMING_METADATA, TreePattern}
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

object StreamingMetadata {
  val metadataOutput: Seq[AttributeReference] = Seq(
    AttributeReference("_latency", IntegerType, nullable = false,
      new MetadataBuilder().putString(METADATA_COL_ATTR_KEY, "_latency").build())()
  )
}

case class StreamingMetadata(child: LogicalPlan, output: Seq[Attribute])
  extends UnaryNode with ExposesMetadataColumns {

  override protected val nodePatterns: Seq[TreePattern] = Seq(STREAMING_METADATA)

  override protected def withNewChildInternal(newChild: LogicalPlan): StreamingMetadata =
    copy(child = newChild)

  override lazy val metadataOutput: Seq[AttributeReference] = StreamingMetadata.metadataOutput

  override def withMetadataColumns(): LogicalPlan = {
    val newMetadata = metadataOutput.filterNot(outputSet.contains)
    println(s"[NEIL] newMetadata called for StreamingMetadata: ${newMetadata}")
    if (newMetadata.nonEmpty) {
      copy(output = output ++ newMetadata)
    } else {
      this
    }
  }
}
