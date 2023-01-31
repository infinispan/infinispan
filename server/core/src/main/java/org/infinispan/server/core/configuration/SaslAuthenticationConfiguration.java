package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslAuthenticationConfiguration implements AuthenticationConfiguration {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SaslAuthenticationConfiguration.class, SECURITY_REALM);
   }

   private final AttributeSet attributes;
   private final boolean enabled;
   private final SaslConfiguration saslConfiguration;

   SaslAuthenticationConfiguration(AttributeSet attributes, SaslConfiguration saslConfiguration, boolean enabled) {
      this.attributes = attributes.checkProtection();
      this.saslConfiguration = saslConfiguration;
      this.enabled = enabled;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean enabled() {
      return enabled;
   }

   public SaslConfiguration sasl() {
      return saslConfiguration;
   }

   @Override
   public String toString() {
      return "AuthenticationConfiguration{" +
            "attributes=" + attributes +
            ", enabled=" + enabled +
            ", sasl=" + saslConfiguration +
            '}';
   }
}
