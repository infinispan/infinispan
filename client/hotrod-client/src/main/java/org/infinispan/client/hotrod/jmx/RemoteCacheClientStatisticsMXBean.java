package org.infinispan.client.hotrod.jmx;

/**
 * RemoteCache client-side statistics (such as number of connections)
 */
public interface RemoteCacheClientStatisticsMXBean {

   /**
    * Returns the number of hits for a remote cache.
    */
   long getRemoteHits();

   /**
    * Returns the number of misses for a remote cache.
    */
   long getRemoteMisses();

   /**
    * Returns the average read time, in milliseconds, for a remote cache.
    */
   long getAverageRemoteReadTime();

   /**
    * Returns the number of remote cache stores (put, replace) that the client applied.
    * Failed conditional operations do not increase the count of entries in the remote cache. Put operations always increase the count even if an operation replaces an equal value.
    */
   long getRemoteStores();

   /**
    * Returns the average store time, in milliseconds, for a remote cache.
    */
   long getAverageRemoteStoreTime();

   /**
    * Returns the number of removes for a remote cache.
    */
   long getRemoteRemoves();

   /**
    * Returns the average time, in milliseconds, for remove operations in a remote cache.
    */
   long getAverageRemoteRemovesTime();

   /**
    * Returns the number of near-cache hits. Returns a value of 0 if near-caching is disabled.
    */
   long getNearCacheHits();

   /**
    * Returns the number of near-cache misses. Returns a value of 0 if near-caching is disabled.
    */
   long getNearCacheMisses();

   /**
    * Returns the number of near-cache invalidations. Returns a value of 0 if near-caching is disabled.
    */
   long getNearCacheInvalidations();

   /**
    * Returns the number of entries currently stored in the near-cache. Returns a value of 0 if near-caching is disabled.
    */
   long getNearCacheSize();

   /**
    * Resets statistics.
    */
   void resetStatistics();

   /**
    * Returns the time, in seconds, since the last reset. See {@link #resetStatistics()}
    */
   long getTimeSinceReset();
}
