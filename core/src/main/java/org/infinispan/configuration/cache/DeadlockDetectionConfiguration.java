package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Configures deadlock detection.
 *
 * @deprecated Since 9.0, deadlock detection is always disabled.
 */
@Deprecated
public class DeadlockDetectionConfiguration implements Matchable<DeadlockDetectionConfiguration>, ConfigurationInfo {
   @Deprecated
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   @Deprecated
   public static final AttributeDefinition<Long> SPIN_DURATION = AttributeDefinition.builder("spinDuration", TimeUnit.MILLISECONDS.toMillis(-1)).immutable().build();
   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("", false);

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

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    *
    * @deprecated Since 9.0, always returns {@code -1}.
    */
   @Deprecated
   public long spinDuration() {
      return spinDuration.get();
   }

   /**
    * Whether deadlock detection is enabled or disabled
    *
    * @deprecated Since 9.0, always returns {@code false}.
    */
   @Deprecated
   public boolean enabled() {
      return enabled.get();
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
