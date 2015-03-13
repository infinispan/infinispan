package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Compatibility mode configuration
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class CompatibilityModeConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(CompatibilityModeConfiguration.class, ENABLED, MARSHALLER);
   }
   private final AttributeSet attributes;

   CompatibilityModeConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   public Marshaller marshaller() {
      return attributes.attribute(MARSHALLER).asObject(Marshaller.class);
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "CompatibilityModeConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CompatibilityModeConfiguration other = (CompatibilityModeConfiguration) obj;
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
