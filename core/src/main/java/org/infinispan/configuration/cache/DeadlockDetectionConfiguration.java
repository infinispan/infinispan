package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Long> SPIN_DURATION = AttributeDefinition.builder("spinDuration", TimeUnit.MILLISECONDS.toMillis(100)).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DeadlockDetectionConfiguration.class, ENABLED, SPIN_DURATION);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Long> spinDuration;
   private final AttributeSet attributes;

   DeadlockDetectionConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      spinDuration = attributes.attribute(SPIN_DURATION);
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public long spinDuration() {
      return attributes.attribute(SPIN_DURATION).get();
   }

   /**
    * Whether deadlock detection is enabled or disabled
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "DeadlockDetectionConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      DeadlockDetectionConfiguration other = (DeadlockDetectionConfiguration) obj;
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
