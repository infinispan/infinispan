package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes, this element is
 * ignored.
 */

public class L1Configuration extends ConfigurationElement<L1Configuration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> INVALIDATION_THRESHOLD = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INVALIDATION_THRESHOLD, 0).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Long> LIFESPAN = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.L1_LIFESPAN, TimeUnit.MINUTES.toMillis(10)).immutable().build();

   public static final AttributeDefinition<Long> CLEANUP_TASK_FREQUENCY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INVALIDATION_CLEANUP_TASK_FREQUENCY, TimeUnit.MINUTES.toMillis(1)).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(L1Configuration.class, ENABLED, INVALIDATION_THRESHOLD, LIFESPAN, CLEANUP_TASK_FREQUENCY);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Integer> invalidationThreshold;
   private final Attribute<Long> lifespan;
   private final Attribute<Long> cleanupTaskFrequency;

   L1Configuration(AttributeSet attributes) {
      super(Element.L1, attributes);
      enabled = attributes.attribute(ENABLED);
      invalidationThreshold = attributes.attribute(INVALIDATION_THRESHOLD);
      lifespan = attributes.attribute(LIFESPAN);
      cleanupTaskFrequency = attributes.attribute(CLEANUP_TASK_FREQUENCY);
   }

   public boolean enabled() {
      return enabled.get();
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
    * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to 0, then multicast
    * will be always be used.
    * </p>
    */
   public int invalidationThreshold() {
      return invalidationThreshold.get();
   }

   /**
    * Determines how often a cleanup thread runs to clean up an internal log of requestors for a specific key
    */
   public long cleanupTaskFrequency() {
      return cleanupTaskFrequency.get();
   }


   /**
    * Maximum lifespan of an entry placed in the L1 cache. Default 10 minutes.
    */
   public long lifespan() {
      return lifespan.get();
   }
}
