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
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    */
   public EvictionStrategy strategy() {
      return strategy;
   }
   
   /**
    * Threading policy for eviction.
    */
   public EvictionThreadPolicy threadPolicy() {
      return threadPolicy;
   }
   
   /**
    * Maximum number of entries in a cache instance. Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    */
   public int maxEntries() {
      return maxEntries;
   }

   @Override
   public String toString() {
      return "EvictionConfiguration{" +
            "maxEntries=" + maxEntries +
            ", strategy=" + strategy +
            ", threadPolicy=" + threadPolicy +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EvictionConfiguration that = (EvictionConfiguration) o;

      if (maxEntries != that.maxEntries) return false;
      if (strategy != that.strategy) return false;
      if (threadPolicy != that.threadPolicy) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = maxEntries;
      result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
      result = 31 * result + (threadPolicy != null ? threadPolicy.hashCode() : 0);
      return result;
   }

}
