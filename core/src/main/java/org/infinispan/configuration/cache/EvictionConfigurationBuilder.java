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
      if (!strategy.isEnabled() && getBuilder().loaders().passivation())
         log.passivationWithoutEviction();
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
