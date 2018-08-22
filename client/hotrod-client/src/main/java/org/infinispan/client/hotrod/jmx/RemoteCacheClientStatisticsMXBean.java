package org.infinispan.client.hotrod.jmx;

public interface RemoteCacheClientStatisticsMXBean {

   /**
    * Returns the number of remote cache hits
    */
   long getRemoteHits();

   /**
    * Returns the number of remote cache misses
    */
   long getRemoteMisses();

   /**
    * Returns the average read time
    */
   long getAverageRemoteReadTime();

   /**
    * Returns the number of remote cache stores (put, replace) that were applied - e.g. failed conditional operations
    * don't increase the count. Put counts all the time, though, even if it replaced an equal value.
    */
   long getRemoteStores();

   /**
    * Returns the average write time
    */
   long getAverageRemoteStoreTime();

   /**
    * Returns the number of remote cache removes
    */
   long getRemoteRemoves();


   /**
    * Returns the average remove time
    */
   long getAverageRemoteRemovesTime();

   /**
    * Returns the number of near-cache hits. If near-caching is disabled this will always return 0
    */
   long getNearCacheHits();

   /**
    * Returns the number of near-cache misses. If near-caching is disabled this will always return 0
    */
   long getNearCacheMisses();

   /**
    * Returns the number of near-cache invalidations. If near-caching is disabled this will always return 0
    */
   long getNearCacheInvalidations();

   /**
    * Returns the number of entries currently stored in the near-cache. If near-caching is disabled this will always
    */
   long getNearCacheSize();

   /**
    * Resets statistics
    */
   void resetStatistics();

   /**
    * Returns the time in seconds since the last reset. See {@link #resetStatistics()}
    */
   long getTimeSinceReset();
}
