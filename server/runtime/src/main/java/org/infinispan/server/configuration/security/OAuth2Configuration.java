package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.PasswordSerializer;

/**
 * @since 10.0
 */
public class OAuth2Configuration implements ConfigurationInfo {

   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder("clientId", null, String.class).build();
   static final AttributeDefinition<String> CLIENT_SECRET = AttributeDefinition.builder("clientSecret", null, String.class).serializer(PasswordSerializer.INSTANCE).build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder("clientSslContext", null, String.class).build();
   static final AttributeDefinition<String> HOST_VERIFICATION_POLICY = AttributeDefinition.builder("hostNameVerificationPolicy", null, String.class).build();
   static final AttributeDefinition<String> INTROSPECTION_URL = AttributeDefinition.builder("introspectionUrl", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, CLIENT_ID, CLIENT_SECRET, INTROSPECTION_URL, HOST_VERIFICATION_POLICY);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.OAUTH2_INTROSPECTION.toString());
   private final AttributeSet attributes;

   OAuth2Configuration(AttributeSet attributes) {
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

   public String clientId() {
      return attributes.attribute(CLIENT_ID).get();
   }
}
