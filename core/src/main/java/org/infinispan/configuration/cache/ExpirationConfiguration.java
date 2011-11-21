package org.infinispan.configuration.cache;

/**
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfiguration {

   private final long lifespan;
   private final long maxIdle;
   private final boolean reaperEnabled;
   private final long wakeUpInterval;

   ExpirationConfiguration(long lifespan, long maxIdle, boolean reaperEnabled, long wakeUpInterval) {
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.reaperEnabled = reaperEnabled;
      this.wakeUpInterval = wakeUpInterval;
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long lifespan() {
      return lifespan;
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long maxIdle() {
      return maxIdle;
   }

   /**
    * Determines whether the background reaper thread is enabled to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public boolean reaperEnabled() {
      return reaperEnabled;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public long wakeUpInterval() {
      return wakeUpInterval;
   }

}
