package org.infinispan.stats;

/**
 * Stats.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Stats {

   /**
    * @return Number of seconds since cache started.
    */
   long getTimeSinceStart();

   /**
    * @return Number of seconds since stats where reset
    */
   long getTimeSinceReset();

   /**
    * Returns the number of entries currently in this cache instance. When
    * the cache is configured with distribution, this method only returns the
    * number of entries in the local cache instance. In other words, it does
    * not attempt to communicate with other nodes to find out about the data
    * stored in other nodes in the cluster that is not available locally.
    *
    * @return Number of entries currently in the cache.
    */
   int getCurrentNumberOfEntries();

   /**
    * Number of entries stored in cache since the cache started running.
    */
   long getTotalNumberOfEntries();

   /**
    * @return Number of put operations on the cache.
    */
   long getStores();

   /**
    * @return Number of get operations.
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
    * @return Average number of milliseconds for a cache put on the cache
    */
   long getAverageWriteTime();

   /**
    * @return Average number of milliseconds for a cache put on the cache
    */
   long getAverageRemoveTime();

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
