package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class JwtConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<List> AUDIENCE = AttributeDefinition.builder("audience", null, List.class).build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder("clientSslContext", null, String.class).build();
   static final AttributeDefinition<String> HOST_NAME_VERIFICATION_POLICY = AttributeDefinition.builder("hostNameVerificationPolicy", null, String.class).build();
   static final AttributeDefinition<List> ISSUER = AttributeDefinition.builder("issuer", null, List.class).build();
   static final AttributeDefinition<Long> JKU_TIMEOUT = AttributeDefinition.builder("jkuTimeout", null, Long.class).build();
   static final AttributeDefinition<String> PUBLIC_KEY = AttributeDefinition.builder("publicKey", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JwtConfiguration.class, AUDIENCE, CLIENT_SSL_CONTEXT, HOST_NAME_VERIFICATION_POLICY, ISSUER, JKU_TIMEOUT, PUBLIC_KEY);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.JWT.toString());

   private final AttributeSet attributes;

   JwtConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JwtConfiguration that = (JwtConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "JwtConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
