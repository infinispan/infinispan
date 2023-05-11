package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.core.configuration.AuthenticationConfiguration;
import org.infinispan.server.resp.authentication.RespAuthenticator;

/**
 * RespAuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
public class RespAuthenticationConfiguration extends ConfigurationElement<RespAuthenticationConfiguration> implements AuthenticationConfiguration {
   public static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).immutable().build();

   private final boolean enabled;
   private final RespAuthenticator authenticator;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RespAuthenticationConfiguration.class, SECURITY_REALM);
   }

   RespAuthenticationConfiguration(AttributeSet attributes, RespAuthenticator authenticator, boolean enabled) {
      super("authentication", attributes);
      this.enabled = enabled;
      this.authenticator = authenticator;
   }

   public boolean enabled() {
      return enabled;
   }

   public RespAuthenticator authenticator() {
      return authenticator;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }
}
