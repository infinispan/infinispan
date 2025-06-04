package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.L1Configuration.CLEANUP_TASK_FREQUENCY;
import static org.infinispan.configuration.cache.L1Configuration.ENABLED;
import static org.infinispan.configuration.cache.L1Configuration.INVALIDATION_THRESHOLD;
import static org.infinispan.configuration.cache.L1Configuration.LIFESPAN;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.eviction.EvictionStrategy;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1ConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<L1Configuration> {
   private final AttributeSet attributes;

   L1ConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = L1Configuration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * <p>
    * Determines whether a multicast or a web of unicasts are used when performing L1 invalidations.
    * </p>
    *
    * <p>
    * By default multicast will be used.
    * </p>
    *
    * <p>
    * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to
    * 0, then multicast will be always be used.
    * </p>
    *
    * @param invalidationThreshold the threshold over which to use a multicast
    *
    */
   public L1ConfigurationBuilder invalidationThreshold(int invalidationThreshold) {
      attributes.attribute(INVALIDATION_THRESHOLD).set(invalidationThreshold);
      return this;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long lifespan) {
      TimeQuantity quantity = TimeQuantity.valueOf(lifespan);
      attributes.attribute(LIFESPAN).set(quantity);
      return enabled(quantity.longValue() > 0);
   }

   /**
    * Same as {@link #lifespan(long)} but supporting time units
    */
   public L1ConfigurationBuilder lifespan(String lifespan) {
      TimeQuantity quantity = TimeQuantity.valueOf(lifespan);
      attributes.attribute(LIFESPAN).set(quantity);
      return enabled(quantity.longValue() > 0);
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long lifespan, TimeUnit unit) {
      return lifespan(unit.toMillis(lifespan));
   }

   /**
    * How often the L1 requestors map is cleaned up of stale items
    */
   public L1ConfigurationBuilder cleanupTaskFrequency(long frequency) {
      attributes.attribute(CLEANUP_TASK_FREQUENCY).set(TimeQuantity.valueOf(frequency));
      return this;
   }

   /**
    * Same as {@link #cleanupTaskFrequency(long)} but supporting time units.
    */
   public L1ConfigurationBuilder cleanupTaskFrequency(String frequency) {
      attributes.attribute(CLEANUP_TASK_FREQUENCY).set(TimeQuantity.valueOf(frequency));
      return this;
   }

   /**
    * How often the L1 requestors map is cleaned up of stale items
    */
   public L1ConfigurationBuilder cleanupTaskFrequency(long frequencyMillis, TimeUnit unit) {
      return cleanupTaskFrequency(unit.toMillis(frequencyMillis));
   }

   public L1ConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public L1ConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public L1ConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get()) {
         if (!clustering().cacheMode().isDistributed())
            throw CONFIG.l1OnlyForDistributedCache(clustering().cacheMode().friendlyCacheModeString());

         if (attributes.attribute(LIFESPAN).get().longValue() < 1)
            throw CONFIG.l1InvalidLifespan();

         MemoryConfigurationBuilder memoryConfigurationBuilder = getClusteringBuilder().memory();
         if (memoryConfigurationBuilder.whenFull() == EvictionStrategy.EXCEPTION) {
            throw CONFIG.l1NotValidWithExpirationEviction();
         }
      }
   }

   @Override
   public L1Configuration create() {
      return new L1Configuration(attributes.protect());
   }

   @Override
   public L1ConfigurationBuilder read(L1Configuration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "L1ConfigurationBuilder [attributes=" + attributes + "]";
   }
}
