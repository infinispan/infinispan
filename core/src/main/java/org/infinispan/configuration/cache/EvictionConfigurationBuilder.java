package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfigurationBuilder extends AbstractConfigurationChildBuilder<EvictionConfiguration> {

   private static final Log log = LogFactory.getLog(EvictionConfigurationBuilder.class);
   
   private int maxEntries = -1;
   private EvictionStrategy strategy = EvictionStrategy.NONE;
   private EvictionThreadPolicy threadPolicy = EvictionThreadPolicy.DEFAULT;
   
   EvictionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   
   /**
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
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
    * Maximum number of entries in a cache instance. Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    * 
    * @param maxEntries
    */
   public EvictionConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   @Override
   void validate() {
      if (!strategy.isEnabled() && getBuilder().loaders().passivation())
         log.passivationWithoutEviction();
      if(strategy == EvictionStrategy.FIFO)
         log.warn("FIFO strategy is deprecated, LRU will be used instead");
      if (strategy.isEnabled() && maxEntries <= 0)
         throw new ConfigurationException("Eviction maxEntries value cannot be less than or equal to zero if eviction is enabled");
   }

   @Override
   EvictionConfiguration create() {
      return new EvictionConfiguration(maxEntries, strategy, threadPolicy);
   }
   
   @Override
   public EvictionConfigurationBuilder read(EvictionConfiguration template) {
      this.maxEntries = template.maxEntries();
      this.strategy = template.strategy();
      this.threadPolicy = template.threadPolicy();
      
      return this;
   }

}
