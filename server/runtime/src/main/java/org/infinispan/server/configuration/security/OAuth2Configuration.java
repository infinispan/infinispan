package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class OAuth2Configuration extends ConfigurationElement<OAuth2Configuration> {

   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder(Attribute.CLIENT_ID, null, String.class).build();
   static final AttributeDefinition<char[]> CLIENT_SECRET = AttributeDefinition.builder(Attribute.CLIENT_SECRET, null, char[].class).serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).build();
   static final AttributeDefinition<String> HOST_VERIFICATION_POLICY = AttributeDefinition.builder(Attribute.HOST_NAME_VERIFICATION_POLICY, null, String.class).build();
   static final AttributeDefinition<String> INTROSPECTION_URL = AttributeDefinition.builder(Attribute.INTROSPECTION_URL, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, CLIENT_ID, CLIENT_SECRET, INTROSPECTION_URL, HOST_VERIFICATION_POLICY);
   }

   OAuth2Configuration(AttributeSet attributes) {
      super(Element.OAUTH2_INTROSPECTION, attributes);
   }

   public String clientId() {
      return attributes.attribute(CLIENT_ID).get();
   }
}
