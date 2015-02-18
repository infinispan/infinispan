package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1Configuration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Integer> INVALIDATION_THRESHOLD = AttributeDefinition.builder("invalidationThreshold", 0).immutable().build();
   static final AttributeDefinition<Long> LIFESPAN = AttributeDefinition.builder("lifespan", TimeUnit.MINUTES.toMillis(10)).immutable().build();
   static final AttributeDefinition<Long> CLEANUP_TASK_FREQUENCY = AttributeDefinition.builder("cleanupTaskFrequency", TimeUnit.MINUTES.toMillis(1)).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(L1Configuration.class, ENABLED, INVALIDATION_THRESHOLD, LIFESPAN, CLEANUP_TASK_FREQUENCY);
   }
   private final AttributeSet attributes;

   L1Configuration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
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
      return attributes.attribute(INVALIDATION_THRESHOLD).asInteger();
   }

   /**
    * Determines how often a cleanup thread runs to clean up an internal log of requestors for a specific key
    */
   public long cleanupTaskFrequency() {
      return attributes.attribute(CLEANUP_TASK_FREQUENCY).asLong();
   }


   /**
    * Maximum lifespan of an entry placed in the L1 cache. Default 10 minutes.
    */
   public long lifespan() {
      return attributes.attribute(LIFESPAN).asLong();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "L1Configuration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      L1Configuration other = (L1Configuration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
