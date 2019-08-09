package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class SniConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("securityRealm", null, String.class).build();
   static final AttributeDefinition<String> HOST_NAME = AttributeDefinition.builder("hostName", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("sni");

   private final AttributeSet attributes;

   SniConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SniConfiguration.class, HOST_NAME, SECURITY_REALM);
   }

   public String realm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public String host() {
      return attributes.attribute(HOST_NAME).get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SniConfiguration that = (SniConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "SniConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
