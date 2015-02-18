package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.L1Configuration.*;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1ConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<L1Configuration> {
   private final static Log log = LogFactory.getLog(L1ConfigurationBuilder.class, Log.class);
   private final AttributeSet attributes;

   L1ConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = L1Configuration.attributeDefinitionSet();
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
      attributes.attribute(LIFESPAN).set(lifespan);
      return this;
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
   public L1ConfigurationBuilder cleanupTaskFrequency(long frequencyMillis) {
      attributes.attribute(CLEANUP_TASK_FREQUENCY).set(frequencyMillis);
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
            throw log.l1OnlyForDistributedCache(clustering().cacheMode().friendlyCacheModeString());

         if (attributes.attribute(LIFESPAN).get() < 1)
            throw log.l1InvalidLifespan();

      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public L1Configuration create() {
      return new L1Configuration(attributes.protect());
   }

   @Override
   public L1ConfigurationBuilder read(L1Configuration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "L1ConfigurationBuilder [attributes=" + attributes + "]";
   }
}
