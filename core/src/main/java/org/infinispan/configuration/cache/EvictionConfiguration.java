package org.infinispan.configuration.cache;

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfiguration {
   
   private final int maxEntries;
   private final EvictionStrategy strategy;
   private final EvictionThreadPolicy threadPolicy;
   
   EvictionConfiguration(int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy threadPolicy) {
      this.maxEntries = maxEntries;
      this.strategy = strategy;
      this.threadPolicy = threadPolicy;
   }
   
   /**
    * Eviction strategy. Available options are 'UNORDERED', 'FIFO', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    *
    * @param strategy
    */
   public EvictionStrategy getStrategy() {
      return strategy;
   }
   
   /**
    * Threading policy for eviction.
    *
    * @param threadPolicy
    */
   public EvictionThreadPolicy getThreadPolicy() {
      return threadPolicy;
   }
   
   /**
    * Maximum number of entries in a cache instance. If selected value is not a power of two the
    * actual value will default to the least power of two larger than selected value. -1 means no
    * limit.
    *
    * @param maxEntries
    */
   public int getMaxEntries() {
      return maxEntries;
   }

}
