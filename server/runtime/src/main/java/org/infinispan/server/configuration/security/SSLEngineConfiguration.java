package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class SSLEngineConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String[]> ENABLED_PROTOCOLS = AttributeDefinition.builder("enabledProtocols", null, String[].class).build();
   static final AttributeDefinition<String> ENABLED_CIPHERSUITES = AttributeDefinition.builder("enabledCiphersuites", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.ENGINE.toString());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SSLEngineConfiguration.class, ENABLED_PROTOCOLS, ENABLED_CIPHERSUITES);
   }

   private final AttributeSet attributes;

   SSLEngineConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public String[] enabledProtocols() {
      return attributes.attribute(ENABLED_PROTOCOLS).get();
   }

   public String enabledCiphersuites() {
      return attributes.attribute(ENABLED_CIPHERSUITES).get();
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

      SSLEngineConfiguration that = (SSLEngineConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "SslEngineConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
