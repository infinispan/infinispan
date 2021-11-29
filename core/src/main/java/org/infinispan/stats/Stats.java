package org.infinispan.stats;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Stats.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Stats extends JsonSerialization {

   /**
    * @return Number of seconds since cache started.
    */
   long getTimeSinceStart();

   /**
    * @return Number of seconds since stats where reset
    */
   long getTimeSinceReset();

   /**
    * Returns the approximate number of entries (in memory or in persistence) in this cache.
    *
    * When the cache is configured with distribution, this method only returns the
    * number of entries in the local cache instance. In other words, it does
    * not attempt to communicate with other nodes to find out about the data
    * stored in other nodes in the cluster that is not available locally.
    *
    * @return Number of entries currently in the cache, including passivated entries.
    */
   long getApproximateEntries();

   /**
    * The same as {@link #getApproximateEntries()}, however passivated entries are not included.
    */
   long getApproximateEntriesInMemory();

   /**
    * The same as {@link #getApproximateEntries()}, however only entries owned as primary are counted.
    *
    * This is only different from {@link #getApproximateEntries()} only in distributed and replicated caches.
    */
   long getApproximateEntriesUnique();

   /**
    * Returns the number of entries currently in this cache instance. When
    * the cache is configured with distribution, this method only returns the
    * number of entries in the local cache instance. In other words, it does
    * not attempt to communicate with other nodes to find out about the data
    * stored in other nodes in the cluster that is not available locally.
    *
    * @return Number of entries currently in the cache, including passivated entries.
    * @deprecated Since 14.0, please use {@link #getApproximateEntries()} or {@link #getApproximateEntriesUnique()} instead.
    */
   @Deprecated
   int getCurrentNumberOfEntries();

   /**
    * The same as {@link #getCurrentNumberOfEntries()}, however passivated entries are not included.
    * @deprecated Since 14.0, please use {@link #getApproximateEntriesInMemory()} instead.
    */
   @Deprecated
   int getCurrentNumberOfEntriesInMemory();

   /**
    * Number of entries stored in cache since the cache started running.
    * @deprecated Since 13.0, please use {@link #getStores()} instead
    */
   @Deprecated
   long getTotalNumberOfEntries();

   /**
    * The amount of off-heap memory used by this cache, or -1 if the cache stores data in the heap.
    */
   long getOffHeapMemoryUsed();

   /**
    * Provides how much memory the current eviction algorithm estimates is in use for data. This method will return a
    * number 0 or greater if memory eviction is in use. If memory eviction is not enabled this method will always return -1.
    * @return memory in use or -1 if memory eviction is not enabled
    */
   long getDataMemoryUsed();

   /**
    * @return Number of entries stored in cache since start.
    */
   long getStores();

   /**
    * @return Number of entries read from the cache since start.
    */
   long getRetrievals();

   /**
    * @return Number of cache get hits.
    */
   long getHits();

   /**
    * @return Number of cache get misses.
    */
   long getMisses();

   /**
    * @return Number of cache removal hits.
    */
   long getRemoveHits();

   /**
    * @return Number of cache removal misses.
    */
   long getRemoveMisses();

   /**
    * @return Number of cache eviction.
    */
   long getEvictions();

   /**
    * @return Average number of milliseconds for a cache get on the cache
    */
   long getAverageReadTime();

   /**
    * @return Average number of nanoseconds for a cache get on the cache
    */
   long getAverageReadTimeNanos();

   /**
    * @return Average number of milliseconds for a cache put on the cache
    * @deprecated Since 14.0, please use {@link #getAverageReadTimeNanos()} instead.
    */
   @Deprecated
   long getAverageWriteTime();

   /**
    * @return Average number of milliseconds for a cache put on the cache
    */
   long getAverageWriteTimeNanos();

   /**
    * @return Average number of milliseconds for a cache remove on the cache
    * @deprecated Since 14.0, please use {@link #getAverageWriteTimeNanos()} instead.
    */
   @Deprecated
   long getAverageRemoveTime();

   /**
    * @return Average number of nanoseconds for a cache remove on the cache
    */
   long getAverageRemoveTimeNanos();

   /**
    * @return Required minimum number of nodes to guarantee data consistency
    */
   int getRequiredMinimumNumberOfNodes();

   /**
    * Reset statistics
    */
   void reset();

   /**
    * Enables or disables statistics at runtime.
    *
    * @param enabled boolean indicating whether statistics should be enable or not
    */
   void setStatisticsEnabled(boolean enabled);

}
