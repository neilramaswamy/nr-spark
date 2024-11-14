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

import java.time.Duration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.TTLConfig
import org.apache.spark.sql.types._

/**
 * Any state variable that wants to support TTL must implement this trait,
 * which they can do by extending [[OneToOneTTLState]] or [[OneToManyTTLState]].
 *
 * The only required methods here are ones relating to evicting expired and all
 * state, via clearExpiredStateForAllKeys and clearAllStateForElementKey,
 * respectively. How classes do this is implementation detail, but the general
 * pattern is to use secondary indexes to make sure cleanup scans
 * Θ(records to evict), not Θ(all records).
 *
 * There are two broad patterns of implementing stateful variables, and thus
 * there are two broad patterns for implementing TTL. The first is when there
 * is a one-to-one mapping between an element key [1] and a value; the primary
 * and secondary index management for this case is implemented by
 * [[OneToOneTTLState]]. When a single element key can have multiple values,
 * all at which can expire at their own, unique times, then
 * [[OneToManyTTLState]] should be used.
 *
 * In either case, implementations need to use some sort of secondary index
 * that orders element keys by expiration time. This base functionality
 * is provided by methods in this trait that read/write/delete to the
 * so-called "TTL index". It is a secondary index with the layout of
 * (expirationMs, elementKey) -> EMPTY_ROW. The expirationMs is big-endian
 * encoded to allow for efficient range scans to find all expired keys.
 *
 * TTLState (or any abstract sub-classes) should never deal with encoding or
 * decoding UnsafeRows to and from their user-facing types. The stateful variable
 * themselves should be doing this; all other TTLState sub-classes should be concerned
 * only with writing, reading, and deleting UnsafeRows and their associated
 * expirations from the primary and secondary indexes. [2]
 *
 * [1]. You might ask, why call it "element key" instead of "grouping key"?
 *      This is because a single grouping key might have multiple elements, as in
 *      the case of a map, which has composite keys of the form (groupingKey, mapKey).
 *      In the case of ValueState, though, the element key is the grouping key.
 *      To generalize to both cases, this class should always use the term elementKey.)
 *
 * [2]. You might also ask, why design it this way? We want the TTLState abstract
 *      sub-classes to write to both the primary and secondary indexes, since they
 *      both need to stay in sync; co-locating the logic is cleanest.
 */
trait TTLState {
  // Name of the state variable, e.g. the string the user passes to get{Value/List/Map}State
  // in the init() method of a StatefulProcessor.
  def stateName: String

  // The StateStore instance used to store the state. There is only one instance shared
  // among the primary and secondary indexes, since it uses virtual column families
  // to keep the indexes separate.
  def store: StateStore

  // The schema of the primary key for the state variable. For value and list state, this
  // is the grouping key. For map state, this is the composite key of the grouping key and
  // a map key.
  def elementKeySchema: StructType

  // The timestamp at which the batch is being processed. All state variables that have
  // an expiration at or before this timestamp must be cleaned up.
  def batchTimestampMs: Long

  // The configuration for this run of the streaming query. It may change between runs
  // (e.g. user sets ttlConfig1, stops their query, updates to ttlConfig2, and then
  // resumes their query).
  def ttlConfig: TTLConfig

  // A map from metric name to the underlying SQLMetric. This should not be updated
  // by the underlying state variable, as the TTL state implementation should be
  // handling all reads/writes/deletes to the indexes.
  def metrics: Map[String, SQLMetric] = Map.empty

  private final val TTL_INDEX = "$ttl_" + stateName
  private final val TTL_INDEX_KEY_SCHEMA = getSingleKeyTTLRowSchema(elementKeySchema)
  private final val TTL_EMPTY_VALUE_ROW_SCHEMA: StructType =
    StructType(Array(StructField("__empty__", NullType)))

  private final val TTL_ENCODER = new TTLEncoder(elementKeySchema)

  // Empty row used for values
  private final val TTL_EMPTY_VALUE_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  protected final def ttlExpirationMs = StateTTL
    .calculateExpirationTimeForDuration(ttlConfig.ttlDuration, batchTimestampMs)

  store.createColFamilyIfAbsent(
    TTL_INDEX,
    TTL_INDEX_KEY_SCHEMA,
    TTL_EMPTY_VALUE_ROW_SCHEMA,
    RangeKeyScanStateEncoderSpec(TTL_INDEX_KEY_SCHEMA, Seq(0)),
    isInternal = true
  )

  protected def insertIntoTTLIndex(expirationMs: Long, elementKey: UnsafeRow): Unit = {
    val secondaryIndexKey = TTL_ENCODER.encodeTTLRow(expirationMs, elementKey)
    store.put(secondaryIndexKey, TTL_EMPTY_VALUE_ROW, TTL_INDEX)
  }

  protected def deleteFromTTLIndex(expirationMs: Long, elementKey: UnsafeRow): Unit = {
    val secondaryIndexKey = TTL_ENCODER.encodeTTLRow(expirationMs, elementKey)
    store.remove(secondaryIndexKey, TTL_INDEX)
  }

  private[sql] def toTTLRow(ttlKey: UnsafeRow): TTLRow = {
    val expirationMs = ttlKey.getLong(0)
    val elementKey = ttlKey.getStruct(1, TTL_INDEX_KEY_SCHEMA.length)
    TTLRow(elementKey, expirationMs)
  }

  private[sql] def getTTLRows(): Iterator[TTLRow] = {
    store.iterator(TTL_INDEX).map(kv => toTTLRow(kv.key))
  }

  protected def ttlEvictionIterator(): Iterator[TTLRow] = {
    val ttlIterator = store.iterator(TTL_INDEX)

    // Recall that the format is (expirationMs, elementKey) -> TTL_EMPTY_VALUE_ROW, so
    // kv.value doesn't ever need to be used.
    ttlIterator.takeWhile(kv => {
      val expirationMs = kv.key.getLong(0)
      StateTTL.isExpired(expirationMs, batchTimestampMs)
    }).map { kv =>
      store.remove(kv.key, TTL_INDEX)
      toTTLRow(kv.key)
    }
  }


  // Encapsulates a row stored in a TTL index.
  protected case class TTLRow(elementKey: UnsafeRow, expirationMs: Long)

  /**
   * Evicts the state associated with this stateful variable that has expired
   * due to TTL. The eviction applies to all grouping keys, and to all indexes,
   * primary or secondary.
   *
   * This method can be called at any time in the micro-batch execution,
   * as long as it is allowed to complete before subsequent state operations are
   * issued. Operations to the state variable should not be issued concurrently while
   * this is running.
   *
   * (Why? Some cleanup operations leave the state store in an inconsistent state while
   * they are doing cleanup. For example, they may choose to get an iterator over all
   * of the existing values, delete all values, and then re-insert only the non-expired
   * values from the iterator. If a get is issued after the delete happens but before the
   * re-insertion completes, the get could return null even when the value does actually
   * exist.)
   *
   * @return number of values cleaned up.
   */
  def clearExpiredStateForAllKeys(): Long

  /**
   * Clears all of the state for this state variable associated with the primary key
   * elementKey. It is responsible for deleting from the primary index as well as
   * any secondary index(es).
   *
   * If a given state variable has to clean up multiple elementKeys (in MapState, for
   * example, every key in the map is its own elementKey), then this method should
   * be invoked for each of those keys.
   */
  def clearAllStateForElementKey(elementKey: UnsafeRow): Unit
}

/**
 * [[OneToManyTTLState]] is an implementation of [[TTLState]] for stateful variables
 * that associate a single key with multiple values; every value has its own expiration
 * timestamp.
 *
 * We need an efficient way to find all the values that have expired, but we cannot
 * issue point-wise deletes to the elements, since they are merged together using the
 * RocksDB StringAppendOperator for merging. As such, we cannot keep a secondary index
 * on the key (expirationMs, groupingKey, indexInList), since we have no way to delete a
 * specific indexInList from the RocksDB value. (In the future, we could write a custom
 * merge operator that can handle tombstones for deleted indexes, but RocksDB doesn't
 * support custom merge operators written in Java/Scala.)
 *
 * Instead, we manage expiration per grouping key instead. Our secondary index will look
 * like (expirationMs, groupingKey) -> EMPTY_ROW. This way, we can quickly find all the
 * grouping keys that contain at least one element that has expired.
 *
 * There is some trickiness here, though. Suppose we have an element key `k` that
 * has a list with one value `v1` that expires at time `t1`. Our primary index looks like
 * k -> [v1]; our secondary index looks like [(t1, k) -> EMPTY_ROW]. Now, we add another
 * value to the list, `v2`, that expires at time `t2`. The primary index updates to be
 * k -> [v1, v2]. However, how do we update our secondary index? We already have an entry
 * in our secondary index for `k`, but it's prefixed with `t1`, which we don't know at the
 * time of inserting `v2`.
 *
 * So, do we:
 *    1. Blindly add (t2, k) -> EMPTY_ROW to the secondary index?
 *    2. Delete (t1, k) from the secondary index, and then add (t2, k) -> EMPTY_ROW?
 *
 * We prefer option 2 because it avoids us from having many entries in the secondary
 * index for the same grouping key. But when we have (t2, k), how do we know to delete
 * (t1, k)? How do we know that t1 is prefixing k in the secondary index?
 *
 * To solve this, we introduce another index that maps element key to the the
 * minimum expiry timestamp for that element key. It is called the min-expiry index.
 * With it, we can know how to update our secondary index, and we can still range-scan
 * the TTL index to find all the keys that have expired.
 *
 * In our previous example, we'd use this min-expiry index to tell us whether `t2` was
 * smaller than the minimum expiration on-file for `k`. If it were smaller, we'd update
 * the TTL index, and the min-expiry index. If not, then we'd just update the primary
 * index. When the batchTimestampMs exceeded `t1`, we'd know to clean up `k` since it would
 * contain at least one expired value. When iterating through the many values for this `k`,
 * we'd then be able to find the next minimum value to insert back into the secondary and
 * min-expiry index.
 *
 * All of this logic is implemented by updatePrimaryAndSecondaryIndices.
 */
abstract class OneToManyTTLState(
    stateNameArg: String,
    storeArg: StateStore,
    elementKeySchemaArg: StructType,
    ttlConfigArg: TTLConfig,
    batchTimestampMsArg: Long,
    metricsArg: Map[String, SQLMetric]) extends TTLState {
  override def stateName: String = stateNameArg
  override def store: StateStore = storeArg
  override def elementKeySchema: StructType = elementKeySchemaArg
  override def ttlConfig: TTLConfig = ttlConfigArg
  override def batchTimestampMs: Long = batchTimestampMsArg
  override def metrics: Map[String, SQLMetric] = metricsArg

  // Schema of the min index: elementKey -> minExpirationMs
  private val MIN_INDEX = "$min_" + stateName
  private val MIN_INDEX_SCHEMA = elementKeySchema
  private val MIN_INDEX_VALUE_SCHEMA = getExpirationMsRowSchema()

  // Projects a Long into an UnsafeRow
  private val minIndexValueProjector = UnsafeProjection.create(MIN_INDEX_VALUE_SCHEMA)

  // Schema of the entry count index: elementKey -> count
  private val COUNT_INDEX = "$count_" + stateName
  private val COUNT_INDEX_VALUE_SCHEMA: StructType =
    StructType(Seq(StructField("count", LongType, nullable = false)))
  private val countIndexValueProjector = UnsafeProjection.create(COUNT_INDEX_VALUE_SCHEMA)

  // Reused internal row that we use to create an UnsafeRow with the schema of
  // COUNT_INDEX_VALUE_SCHEMA and the desired value. It is not thread safe (although, anyway,
  // this class is not thread safe).
  private val reusedCountIndexValueRow = new GenericInternalRow(1)

  store.createColFamilyIfAbsent(
    MIN_INDEX,
    MIN_INDEX_SCHEMA,
    MIN_INDEX_VALUE_SCHEMA,
    NoPrefixKeyStateEncoderSpec(MIN_INDEX_SCHEMA),
    isInternal = true
  )

  store.createColFamilyIfAbsent(
    COUNT_INDEX,
    elementKeySchema,
    COUNT_INDEX_VALUE_SCHEMA,
    NoPrefixKeyStateEncoderSpec(elementKeySchema),
    isInternal = true
  )


  /**
   * Function to get the number of entries in the list state for a given grouping key
   * @param encodedKey - encoded grouping key
   * @return - number of entries in the list state
   */
  def getEntryCount(elementKey: UnsafeRow): Long = {
    val countRow = store.get(elementKey, COUNT_INDEX)
    if (countRow != null) {
      countRow.getLong(0)
    } else {
      0L
    }
  }

  /**
   * Function to update the number of entries in the list state for a given element key
   * @param elementKey - encoded grouping key
   * @param updatedCount - updated count of entries in the list state
   */
  def updateEntryCount(elementKey: UnsafeRow, updatedCount: Long): Unit = {
    reusedCountIndexValueRow.setLong(0, updatedCount)
    store.put(elementKey,
      countIndexValueProjector(reusedCountIndexValueRow.asInstanceOf[InternalRow]),
      COUNT_INDEX
    )
  }

  /**
   * Function to remove the number of entries in the list state for a given grouping key
   * @param elementKey - encoded element key
   */
  def removeEntryCount(elementKey: UnsafeRow): Unit = {
    store.remove(elementKey, COUNT_INDEX)
  }

  private def writePrimaryIndexEntries(
      overwritePrimaryIndex: Boolean,
      elementKey: UnsafeRow,
      elementValues: Iterator[UnsafeRow]): Unit = {
    val initialEntryCount = if (overwritePrimaryIndex) {
      removeEntryCount(elementKey)
      0
    } else {
      getEntryCount(elementKey)
    }

    // Manually keep track of the count so that we can update the count index. We don't
    // want to call elementValues.size since that will try to re-read the iterator.
    var numNewElements = 0

    // If we're overwriting the primary index, then we only need to put the first value,
    // and then we can merge the rest.
    var isFirst = true
    elementValues.foreach { value =>
      numNewElements += 1
      if (isFirst && overwritePrimaryIndex) {
        isFirst = false
        store.put(elementKey, value, stateName)
      } else {
        store.merge(elementKey, value, stateName)
      }
    }

    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows", numNewElements)
    updateEntryCount(elementKey, initialEntryCount + numNewElements)
  }

  protected def updatePrimaryAndSecondaryIndices(
      overwritePrimaryIndex: Boolean,
      elementKey: UnsafeRow,
      elementValues: Iterator[UnsafeRow],
      expirationMs: Long): Unit = {
    val existingMinExpirationUnsafeRow = store.get(elementKey, MIN_INDEX)

    writePrimaryIndexEntries(overwritePrimaryIndex, elementKey, elementValues)

    // If nothing exists in the secondary index, then we need to make sure to write
    // the primary and the secondary indices. There's nothing to clean-up from the
    // secondary index, since it's empty.
    if (existingMinExpirationUnsafeRow == null) {
      // Insert into the min-expiry and TTL index, in no particular order.
      store.put(elementKey, minIndexValueProjector(InternalRow(expirationMs)), MIN_INDEX)
      insertIntoTTLIndex(expirationMs, elementKey)
    } else {
      val existingMinExpiration = existingMinExpirationUnsafeRow.getLong(0)

      // If we're overwriting the primary index (via a put, not an append), then we need
      // to definitely clear out the secondary index entries. Why? Suppose we had expirations
      // 5, 10, and 15. If we overwrite, then none of those expirations are valid anymore.
      //
      // If we're not overwriting the primary index, there is still a case where we need to
      // modify the secondary index. This is if the new expiration is less than the existing
      // expiration. In that case, we must delete from the TTL index, and then reinsert into
      // the TTL index, and then overwrite the min index.
      if (overwritePrimaryIndex || expirationMs < existingMinExpiration) {
        // We don't actually have to delete from the min index, since we're going
        // to overwrite it on the next line. However, since the TTL index has the existing
        // minimum expiration in it, we need to delete that.
        deleteFromTTLIndex(existingMinExpiration, elementKey)

        // Insert into the min-expiry and TTL index, in no particular order.
        store.put(elementKey, minIndexValueProjector(InternalRow(expirationMs)), MIN_INDEX)
        insertIntoTTLIndex(expirationMs, elementKey)
      }
    }
  }

  // The return type of clearExpiredValues. For a one-to-many stateful variable, cleanup
  // must go through all of the values. numValuesExpired represents the number of entries
  // that were removed (for metrics), and newMinExpirationMs is the new minimum expiration
  // for the values remaining in the state variable.
  case class ValueExpirationResult(
      numValuesExpired: Long,
      newMinExpirationMs: Option[Long])

  // Clears all the expired values for the given elementKey.
  protected def clearExpiredValues(elementKey: UnsafeRow): ValueExpirationResult

  override def clearExpiredStateForAllKeys(): Long = {
    var totalNumValuesExpired = 0L

    // ttlEvictionIterator deletes from the TTL index
    ttlEvictionIterator().foreach { ttlRow =>
      val elementKey = ttlRow.elementKey
      store.remove(elementKey, MIN_INDEX)

      // Now, we need the specific implementation to remove all the values associated with
      // elementKey.
      val valueExpirationResult = clearExpiredValues(elementKey)

      valueExpirationResult.newMinExpirationMs.foreach { newExpirationMs =>
        // Insert into the min-expiry and TTL index, in no particular order.
        store.put(elementKey, minIndexValueProjector(InternalRow(newExpirationMs)), MIN_INDEX)
        insertIntoTTLIndex(newExpirationMs, elementKey)
      }

      // If we have records [foo, bar, baz] and bar and baz are expiring, then, the
      // entryCountBeforeExpirations would be 3. The numValuesExpired would be 2, and so the
      // newEntryCount would be 3 - 2 = 1.
      val entryCountBeforeExpirations = getEntryCount(elementKey)
      val numValuesExpired = valueExpirationResult.numValuesExpired
      val newEntryCount = entryCountBeforeExpirations - numValuesExpired

      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", numValuesExpired)

      if (newEntryCount == 0) {
        removeEntryCount(elementKey)
      } else {
        updateEntryCount(elementKey, newEntryCount)
      }

      totalNumValuesExpired += numValuesExpired
    }

    totalNumValuesExpired
  }

  override def clearAllStateForElementKey(elementKey: UnsafeRow): Unit = {
    val existingMinExpirationUnsafeRow = store.get(elementKey, MIN_INDEX)
    if (existingMinExpirationUnsafeRow != null) {
      val existingMinExpiration = existingMinExpirationUnsafeRow.getLong(0)

      store.remove(elementKey, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", getEntryCount(elementKey))
      removeEntryCount(elementKey)

      store.remove(elementKey, MIN_INDEX)
      deleteFromTTLIndex(existingMinExpiration, elementKey)
    }
  }

  // Exposed for testing.
  private[sql] def minIndexIterator(): Iterator[(UnsafeRow, Long)] = {
    store
      .iterator(MIN_INDEX)
      .map(kv => (kv.key, kv.value.getLong(0)))
  }
}


/**
 * OneToOneTTLState is an implementation of [[TTLState]] that is used to manage
 * TTL for state variables that need a single secondary index to efficiently manage
 * records with an expiration.
 *
 * The primary index for state variables that can use a [[OneToOneTTLState]] have
 * the form of: [elementKey -> (value, elementExpiration)]. You'll notice that, given
 * a timestamp, it would take linear time to probe the primary index for all of its
 * expired values.
 *
 * As a result, this class uses helper methods from [[TTLState]] to maintain the secondary
 * index from [(elementExpiration, elementKey) -> EMPTY_ROW].
 *
 * For an explanation of why this structure is not always sufficient (e.g. why the class
 * [[OneToManyTTLState]] is needed), please visit its class-doc comment.
 */
abstract class OneToOneTTLState(
    stateNameArg: String,
    storeArg: StateStore,
    elementKeySchemaArg: StructType,
    ttlConfigArg: TTLConfig,
    batchTimestampMsArg: Long,
    metricsArg: Map[String, SQLMetric]) extends TTLState {
  override def stateName: String = stateNameArg
  override def store: StateStore = storeArg
  override def elementKeySchema: StructType = elementKeySchemaArg
  override def ttlConfig: TTLConfig = ttlConfigArg
  override def batchTimestampMs: Long = batchTimestampMsArg
  override def metrics: Map[String, SQLMetric] = metricsArg

  /**
   * This method updates the TTL for the given elementKey to be expirationMs,
   * updating both the primary and secondary indices if needed.
   *
   * Note that an elementKey may be the state variable's grouping key, _or_ it
   * could be a composite key. MapState is an example of a state variable that
   * has composite keys, which has the structure of the groupingKey followed by
   * the specific key in the map. This method doesn't need to know what type of
   * key is being used, though, since in either case, it's just an UnsafeRow.
   *
   * @param elementKey the key for which the TTL should be updated, which may
   *                   either be an UnsafeRow derived from [[SingleKeyTTLRow]]
   *                   or [[CompositeKeyTTLRow]].
   * @param expirationMs the new expiration timestamp to use for elementKey.
   */
  def updateIndices(
      elementKey: UnsafeRow,
      elementValue: UnsafeRow,
      expirationMs: Long): Unit = {
    val existingPrimaryValue = store.get(elementKey, stateName)

    // Doesn't exist. Insert into the primary and TTL indexes.
    if (existingPrimaryValue == null) {
      store.put(elementKey, elementValue, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")

      insertIntoTTLIndex(expirationMs, elementKey)
    } else {
      // The new value and the existing one may differ in either timestamp or in actual
      // value. In either case, we need to update the primary index.
      if (elementValue != existingPrimaryValue) {
        store.put(elementKey, elementValue, stateName)
        TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
      }

      // However, the TTL index might already have the correct mapping from expirationMs to
      // elementKey. But if it doesn't, then we need to update the TTL index.
      val existingExpirationMs = existingPrimaryValue.getLong(1)
      if (existingExpirationMs != expirationMs) {
        deleteFromTTLIndex(existingExpirationMs, elementKey)
        insertIntoTTLIndex(expirationMs, elementKey)
      }
    }
  }

  override def clearExpiredStateForAllKeys(): Long = {
    var numValuesExpired = 0L

    // ttlEvictionIterator deletes from the secondary index
    ttlEvictionIterator().foreach { ttlRow =>
      store.remove(ttlRow.elementKey, stateName)
      numValuesExpired += 1
    }

    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", numValuesExpired)
    numValuesExpired
  }

    override def clearAllStateForElementKey(elementKey: UnsafeRow): Unit = {
      val existingPrimaryValue = store.get(elementKey, stateName)
      if (existingPrimaryValue != null) {
        val existingExpirationMs = existingPrimaryValue.getLong(1)

        store.remove(elementKey, stateName)
        TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")

        deleteFromTTLIndex(existingExpirationMs, elementKey)
      }
    }
}

/**
 * Helper methods for user State TTL.
 */
object StateTTL {
  def calculateExpirationTimeForDuration(
      ttlDuration: Duration,
      batchTtlExpirationMs: Long): Long = {
    batchTtlExpirationMs + ttlDuration.toMillis
  }

  def isExpired(
      expirationMs: Long,
      batchTtlExpirationMs: Long): Boolean = {
    batchTtlExpirationMs >= expirationMs
  }
}
