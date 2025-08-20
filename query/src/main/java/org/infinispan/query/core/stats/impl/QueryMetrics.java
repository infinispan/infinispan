package org.infinispan.query.core.stats.impl;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Store statistics for a certain kind of query.
 *
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.QUERY_METRICS)
class QueryMetrics implements JsonSerialization {
   final LongAdder count = new LongAdder();
   final LongAdder totalTime = new LongAdder();
   final AtomicLong maxTime = new AtomicLong(-1);
   volatile String slowest = null;

   final ReadWriteLock lock = new ReentrantReadWriteLock();
   final Lock recordLock = lock.readLock();
   final Lock summaryLock = lock.writeLock();

   public QueryMetrics() {
      count.reset();
      totalTime.reset();
   }

   @ProtoFactory
   public QueryMetrics(long count, long totalTime, long maxTime, String slowest) {
      this.count.add(count);
      this.totalTime.add(totalTime);
      this.maxTime.set(maxTime);
      this.slowest = slowest;
   }

   @ProtoField(number = 1, defaultValue = "0")
   long count() {
      return count.longValue();
   }

   @ProtoField(number = 2, defaultValue = "0")
   long totalTime() {
      return totalTime.longValue();
   }

   @ProtoField(number = 3, defaultValue = "0")
   long maxTime() {
      return maxTime.get();
   }

   @ProtoField(number = 4)
   public String slowest() {
      return slowest;
   }

   double avg() {
      summaryLock.lock();
      try {
         long countValue = count.longValue();
         if (countValue == 0) return 0;
         return this.totalTime.doubleValue() / count.longValue();
      } finally {
         summaryLock.unlock();
      }
   }

   void record(String q, long timeNanos) {
      recordLock.lock();
      try {
         count.increment();
         totalTime.add(timeNanos);
         updateMaxQuery(q, timeNanos);
      } finally {
         recordLock.unlock();
      }

   }

   void record(long timeNanos) {
      record(null, timeNanos);
   }

   void clear() {
      count.reset();
      totalTime.reset();
      maxTime.set(0);
      slowest = null;
   }

   private void updateMaxQuery(String q, long time) {
      long localMax = maxTime.get();
      while (time > localMax) {
         if (maxTime.compareAndSet(localMax, time)) {
            slowest = q;
            return;
         }
         localMax = maxTime.get();
      }
   }

   @Override
   public Json toJson() {
      Json object = Json.object().set("count", count()).set("average", avg()).set("max", maxTime());
      if (slowest != null) object.set("slowest", slowest());
      return object;
   }
}
