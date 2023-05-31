package org.infinispan.configuration.cache;


/**
 * Cache replication mode.
 */
public enum CacheType {
   LOCAL,
   REPLICATION,
   INVALIDATION,
   DISTRIBUTION,
   SCATTERED;

   private static final CacheType[] cachedValues = values();

   public static CacheType valueOf(int order) {
      return cachedValues[order];
   }
}
