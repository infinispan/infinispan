package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;

/**
 * @since 15.0
 **/
public class TextAuthenticationConfiguration extends ConfigurationElement<TextAuthenticationConfiguration> {
   static final AttributeDefinition<UsernamePasswordAuthenticator> AUTHENTICATOR = AttributeDefinition.builder("authenticator", null, UsernamePasswordAuthenticator.class).autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TextAuthenticationConfiguration.class, AUTHENTICATOR);
   }

   protected TextAuthenticationConfiguration(AttributeSet attributes) {
      super("authenticator", attributes);
   }

   public UsernamePasswordAuthenticator authenticator() {
      return attributes.attribute(AUTHENTICATOR).get();
   }
}
