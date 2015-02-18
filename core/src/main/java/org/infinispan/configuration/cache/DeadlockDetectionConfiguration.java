package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Long> SPIN_DURATION = AttributeDefinition.builder("spinDuration", TimeUnit.MILLISECONDS.toMillis(100)).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(DeadlockDetectionConfiguration.class, ENABLED, SPIN_DURATION);
   }
   private final AttributeSet attributes;

   DeadlockDetectionConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public long spinDuration() {
      return attributes.attribute(SPIN_DURATION).asLong();
   }

   /**
    * Whether deadlock detection is enabled or disabled
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DeadlockDetectionConfiguration that = (DeadlockDetectionConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

}
