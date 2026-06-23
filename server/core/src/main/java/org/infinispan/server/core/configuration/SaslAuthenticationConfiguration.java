package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslAuthenticationConfiguration extends ConfigurationElement<SaslAuthenticationConfiguration> implements AuthenticationConfiguration {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false, Boolean.class).autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SaslAuthenticationConfiguration.class, SECURITY_REALM, ENABLED);
   }

   private final SaslConfiguration saslConfiguration;

   SaslAuthenticationConfiguration(AttributeSet attributes, SaslConfiguration saslConfiguration) {
      super("authentication",  attributes, saslConfiguration);
      this.saslConfiguration = saslConfiguration;
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
}
