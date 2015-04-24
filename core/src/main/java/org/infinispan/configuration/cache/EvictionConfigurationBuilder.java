package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.EvictionConfiguration.TYPE;
import static org.infinispan.configuration.cache.EvictionConfiguration.SIZE;
import static org.infinispan.configuration.cache.EvictionConfiguration.STRATEGY;
import static org.infinispan.configuration.cache.EvictionConfiguration.THREAD_POLICY;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<EvictionConfiguration> {
   private static final Log log = LogFactory.getLog(EvictionConfigurationBuilder.class);
   private final AttributeSet attributes;

   public static final long EVICTION_MAX_SIZE = 0x00ffffffffffffffl;

   EvictionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = EvictionConfiguration.attributeDefinitionSet();
   }

   /**
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    *
    * @param evictionStrategy
    */
   public EvictionConfigurationBuilder strategy(EvictionStrategy evictionStrategy) {
      attributes.attribute(STRATEGY).set(evictionStrategy);
      return this;
   }

   EvictionStrategy strategy() {
      return attributes.attribute(STRATEGY).get();
   }

   /**
    * Threading policy for eviction.
    *
    * @param policy
    */
   public EvictionConfigurationBuilder threadPolicy(EvictionThreadPolicy policy) {
      attributes.attribute(THREAD_POLICY).set(policy);
      return this;
   }

   /**
    * Maximum number of entries in a cache instance. Backward-compatible shortcut for
    * type(EvictionType.COUNT).size(maxEntries);
    *
    * @param maxEntries
    */
   public EvictionConfigurationBuilder maxEntries(long maxEntries) {
      return type(EvictionType.COUNT).size(maxEntries);
   }

   @Deprecated
   public EvictionConfigurationBuilder maxEntries(int maxEntries) {
      return maxEntries((long)maxEntries);
   }

   /**
    * Defines the maximum size before eviction occurs. See {@link #type(EvictionType)}
    *
    * @param size
    */
   public EvictionConfigurationBuilder size(long size) {
      attributes.attribute(SIZE).set(size);
      return this;
   }

   /**
    * Sets the eviction type which can either be
    * <ul>
    * <li>COUNT - entries will be evicted when the number of entries exceeds the {@link #size(long)}</li>
    * <li>MEMORY - entries will be evicted when the approximate combined size of all values exceeds the {@link #size(long)}</li>
    * </ul>
    *
    * Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    *
    * @param type
    */
   public EvictionConfigurationBuilder type(EvictionType type) {
      attributes.attribute(TYPE).set(type);
      return this;
   }

   @Override
   public void validate() {
      EvictionStrategy strategy = attributes.attribute(STRATEGY).get();
      Long maxEntries = attributes.attribute(SIZE).get();
      if (!strategy.isEnabled() && getBuilder().persistence().passivation())
         log.passivationWithoutEviction();
      if(strategy == EvictionStrategy.FIFO)
         log.warnFifoStrategyIsDeprecated();
      if (strategy.isEnabled() && maxEntries <= 0)
         throw new CacheConfigurationException("Eviction maxEntries value cannot be less than or equal to zero if eviction is enabled");
      if (maxEntries > 0 && !strategy.isEnabled()) {
         strategy(EvictionStrategy.LIRS);
         log.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overriden to %s", maxEntries, strategy);
      }
      if (strategy == EvictionStrategy.LIRS && attributes.attribute(TYPE).get() == EvictionType.MEMORY) {
         throw new CacheConfigurationException("Eviction cannot use memory based approximation with LIRS");
      }
      if (maxEntries > EVICTION_MAX_SIZE) {
         throw log.evictionSizeTooLarge(maxEntries);
      }
      if (attributes.attribute(TYPE).get() == EvictionType.MEMORY) {
         String javaVM = SecurityActions.getSystemProperty("java.vm.name");
         if (!javaVM.contains("HotSpot")) {
            log.memoryApproximationUnsupportedVM(javaVM);
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public EvictionConfiguration create() {
      return new EvictionConfiguration(attributes.protect());
   }

   @Override
   public EvictionConfigurationBuilder read(EvictionConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }
}
