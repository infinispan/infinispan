package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes, this element is
 * ignored.
 */

public class L1Configuration extends ConfigurationElement<L1Configuration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> INVALIDATION_THRESHOLD = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INVALIDATION_THRESHOLD, Integer.MAX_VALUE).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Float> INVALIDATION_THRESHOLD_RATIO = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INVALIDATION_THRESHOLD_RATIO, 0.5f).immutable().autoPersist(false).build();
   public static final AttributeDefinition<TimeQuantity> LIFESPAN = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.L1_LIFESPAN, TimeQuantity.valueOf("10m")).immutable().build();

   public static final AttributeDefinition<TimeQuantity> CLEANUP_TASK_FREQUENCY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INVALIDATION_CLEANUP_TASK_FREQUENCY, TimeQuantity.valueOf("1m")).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(L1Configuration.class, ENABLED, INVALIDATION_THRESHOLD, LIFESPAN, CLEANUP_TASK_FREQUENCY);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Integer> invalidationThreshold;
   private final Attribute<Float> invalidationThresholdRatio;
   private final Attribute<TimeQuantity> lifespan;
   private final Attribute<TimeQuantity> cleanupTaskFrequency;

   L1Configuration(AttributeSet attributes) {
      super(Element.L1, attributes);
      enabled = attributes.attribute(ENABLED);
      invalidationThreshold = attributes.attribute(INVALIDATION_THRESHOLD);
      invalidationThresholdRatio = attributes.attribute(INVALIDATION_THRESHOLD_RATIO);
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
    * If the threshold is set to a negative number, then unicasts will always be used. If the threshold is set to a
    * positive number, then multicast will be used if the number of nodes to send to exceeds this number.
    * </p>
    *
    * <p>
    * By default this is not used and {@link #invalidationThresholdRatio()} will be used instead.
    * </p>
    */
   public int invalidationThreshold() {
      return invalidationThreshold.get();
   }

   /**
    * <p>
    * Determines whether a multicast or a web of unicasts are used when performing L1 invalidations.
    * </p>
    *
    * <p>
    * If the threshold is set to a negative number, then unicasts will always be used. If the threshold is set to a
    * positive number, then multicast will be used if the ratio between nodes to send to and total number of nodes
    * exceeds this number.
    * </p>
    *
    * <p>
    * By default multicast will be used if the number of nodes to send exceeds 0.5 of the total number of nodes.
    * </p>
    */
   public float invalidationThresholdRatio() {
      return invalidationThresholdRatio.get();
   }

   /**
    * Determines how often a cleanup thread runs to clean up an internal log of requestors for a specific key
    */
   public long cleanupTaskFrequency() {
      return cleanupTaskFrequency.get().longValue();
   }


   /**
    * Maximum lifespan of an entry placed in the L1 cache. Default 10 minutes.
    */
   public long lifespan() {
      return lifespan.get().longValue();
   }
}
