package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class JwtConfiguration extends ConfigurationElement<JwtConfiguration> {
   static final AttributeDefinition<List> AUDIENCE = AttributeDefinition.builder(Attribute.AUDIENCE, null, List.class).build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).build();
   static final AttributeDefinition<String> HOST_NAME_VERIFICATION_POLICY = AttributeDefinition.builder(Attribute.HOST_NAME_VERIFICATION_POLICY, null, String.class).build();
   static final AttributeDefinition<List> ISSUER = AttributeDefinition.builder(Attribute.ISSUER, null, List.class).build();
   static final AttributeDefinition<Long> JKU_TIMEOUT = AttributeDefinition.builder(Attribute.JKU_TIMEOUT, null, Long.class).build();
   static final AttributeDefinition<String> PUBLIC_KEY = AttributeDefinition.builder(Attribute.PUBLIC_KEY, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JwtConfiguration.class, AUDIENCE, CLIENT_SSL_CONTEXT, HOST_NAME_VERIFICATION_POLICY, ISSUER, JKU_TIMEOUT, PUBLIC_KEY);
   }

   JwtConfiguration(AttributeSet attributes) {
      super(Element.JWT, attributes);
   }
}
