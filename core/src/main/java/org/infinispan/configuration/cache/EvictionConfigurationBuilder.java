package org.infinispan.configuration.cache;

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfigurationBuilder extends AbstractConfigurationChildBuilder<EvictionConfiguration> {

   private int maxEntries = -1;
   private EvictionStrategy strategy = EvictionStrategy.NONE;
   private EvictionThreadPolicy threadPolicy = EvictionThreadPolicy.DEFAULT;
   
   EvictionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   
   /**
    * Eviction strategy. Available options are 'UNORDERED', 'FIFO', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    *
    * @param strategy
    */
   public EvictionConfigurationBuilder strategy(EvictionStrategy evictionStrategy) {
      this.strategy = evictionStrategy;
      return this;
   }
   
   /**
    * Threading policy for eviction.
    *
    * @param threadPolicy
    */
   public EvictionConfigurationBuilder threadPolicy(EvictionThreadPolicy policy) {
      this.threadPolicy = policy;
      return this;
   }
   
   /**
    * Maximum number of entries in a cache instance. If selected value is not a power of two the
    * actual value will default to the least power of two larger than selected value. -1 means no
    * limit.
    *
    * @param maxEntries
    */
   public EvictionConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   EvictionConfiguration create() {
      return new EvictionConfiguration(maxEntries, strategy, threadPolicy);
   }

}
