package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.COMPATIBILITY;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Compatibility mode configuration
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class CompatibilityModeConfiguration implements Matchable<CompatibilityModeConfiguration>, ConfigurationInfo {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CompatibilityModeConfiguration.class, ENABLED, MARSHALLER);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(COMPATIBILITY.getLocalName());

   private final Attribute<Boolean> enabled;
   private final Attribute<Marshaller> marshaller;
   private final AttributeSet attributes;

   CompatibilityModeConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      marshaller = attributes.attribute(MARSHALLER);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public boolean enabled() {
      return enabled.get();
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   public AttributeSet attributes() {
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
