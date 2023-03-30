package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.core.configuration.AuthenticationConfiguration;
import org.infinispan.server.core.configuration.SaslConfiguration;

/**
 * AuthenticationConfiguration.
 *
 * @since 15.0
 */
public class MemcachedAuthenticationConfiguration extends ConfigurationElement<MemcachedAuthenticationConfiguration> implements AuthenticationConfiguration {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemcachedAuthenticationConfiguration.class, SECURITY_REALM, ENABLED);
   }

   private final SaslConfiguration saslConfiguration;
   private final TextAuthenticationConfiguration textAuthenticationConfiguration;

   MemcachedAuthenticationConfiguration(AttributeSet attributes, SaslConfiguration saslConfiguration, TextAuthenticationConfiguration textAuthenticationConfiguration) {
      super("authentication", attributes, saslConfiguration, textAuthenticationConfiguration);
      this.saslConfiguration = saslConfiguration;
      this.textAuthenticationConfiguration = textAuthenticationConfiguration;
   }

   @Override
   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public SaslConfiguration sasl() {
      return saslConfiguration;
   }

   public TextAuthenticationConfiguration text() {
      return textAuthenticationConfiguration;
   }
}
