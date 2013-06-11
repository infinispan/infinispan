/**
 *  Copyright 2011 Terracotta, Inc.
 *  Copyright 2011 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.infinispan.jcache;

import org.infinispan.AdvancedCache;
import org.infinispan.stats.Stats;

import javax.cache.CacheStatisticsMXBean;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * The reference implementation of {@link CacheStatisticsMXBean}.
 */
public class RICacheStatistics implements CacheStatisticsMXBean, Serializable {

   private static final long serialVersionUID = -5589437411679003894L;

   private final AdvancedCache<?, ?> cache;

   // TODO: Replace with getTimeSinceReset?
   private Date lastCollectionStartDate = new Date();

   /**
    * Constructs a cache statistics object
    *
    * @param cache the associated cache
    */
   public RICacheStatistics(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

   /**
    * {@inheritDoc}
    * <p/>
    * Statistics will also automatically be cleared if internal counters
    * overflow.
    */
   @Override
   public void clear() {
      cache.getStats().reset();
      lastCollectionStartDate = new Date();
   }

   /**
    * The date from which statistics have been accumulated. Because statistics
    * can be cleared, this is not necessarily since the cache was started.
    *
    * @return the date statistics started being accumulated
    */
   @Override
   public Date getStartAccumulationDate() {
      return lastCollectionStartDate;
   }

   /**
    * @return the number of hits
    */
   @Override
   public long getCacheHits() {
      return mapToSpecValidStat(cache.getStats().getHits());
   }

   /**
    * Returns cache hits as a percentage of total gets.
    *
    * @return the percentage of successful hits, as a decimal
    */
   @Override
   public float getCacheHitPercentage() {
      long hits = getCacheHits();
      if (hits == 0)
         return 0;

      return hits / getCacheGets();
   }

   /**
    * @return the number of misses
    */
   @Override
   public long getCacheMisses() {
      return mapToSpecValidStat(cache.getStats().getMisses());
   }

   /**
    * Returns cache misses as a percentage of total gets.
    *
    * @return the percentage of accesses that failed to find anything
    */
   @Override
   public float getCacheMissPercentage() {
      long misses = getCacheMisses();
      if (misses == 0)
         return 0;

      return misses / getCacheGets();
   }

   /**
    * The total number of requests to the cache. This will be equal to the sum
    * of the hits and misses.
    * <p/>
    * A "get" is an operation that returns the current or previous value.
    *
    * @return the number of hits
    */
   @Override
   public long getCacheGets() {
      Stats stats = cache.getStats();
      return stats.getHits() + stats.getMisses();
   }

   /**
    * The total number of puts to the cache.
    * <p/>
    * A put is counted even if it is immediately evicted. A replace includes a
    * put and remove.
    *
    * @return the number of hits
    */
   @Override
   public long getCachePuts() {
      return mapToSpecValidStat(cache.getStats().getStores());
   }

   /**
    * The total number of removals from the cache. This does not include
    * evictions, where the cache itself initiates the removal to make space.
    * <p/>
    *
    * @return the number of removals
    */
   @Override
   public long getCacheRemovals() {
      Stats stats = cache.getStats();
      return mapToSpecValidStat(stats.getRemoveHits() + stats.getRemoveMisses());
   }

   /**
    * @return the number of evictions from the cache
    */
   @Override
   public long getCacheEvictions() {
      return mapToSpecValidStat(cache.getStats().getEvictions());
   }

   /**
    * The mean time to execute gets.
    * <p/>
    * In a read-through cache the time taken to load an entry on miss is not included in get time.
    *
    * @return the time in µs
    */
   @Override
   public float getAverageGetTime() {
      return TimeUnit.MILLISECONDS.toMicros(
            mapToSpecValidStat(cache.getStats().getAverageReadTime()));
   }

   /**
    * The mean time to execute puts.
    *
    * @return the time in µs
    */
   @Override
   public float getAveragePutTime() {
      return TimeUnit.MILLISECONDS.toMicros(
            mapToSpecValidStat(cache.getStats().getAverageWriteTime()));
   }

   /**
    * The mean time to execute removes.
    *
    * @return the time in µs
    */
   @Override
   public float getAverageRemoveTime() {
      return TimeUnit.MILLISECONDS.toMicros(
            mapToSpecValidStat(cache.getStats().getAverageRemoveTime()));
   }

   /**
    * Maps an Infinispan statistic to a value that's
    * accepted by the JCache specification.
    *
    * @param stat original Infinispan statistic
    * @return a JCache specification valid statistic
    */
   private long mapToSpecValidStat(long stat) {
      return stat < 0 ? 0 : stat;
   }

}
