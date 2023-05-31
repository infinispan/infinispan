package org.infinispan.configuration.cache;


/**
 * Cache replication mode.
 */
public enum CacheType {
   LOCAL,
   REPLICATION,
   INVALIDATION,
   DISTRIBUTION;

   private static final CacheType[] cachedValues = values();

   public static CacheType valueOf(int order) {
      return cachedValues[order];
   }
}
