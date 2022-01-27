package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.resp.Authenticator;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
public class AuthenticationConfiguration extends ConfigurationElement<AuthenticationConfiguration> {
   public static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).immutable().build();

   private final Boolean enabled;
   private final Authenticator authenticator;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, SECURITY_REALM);
   }

   AuthenticationConfiguration(AttributeSet attributes, Authenticator authenticator, Boolean enabled) {
      super("authentication", attributes);
      this.enabled = enabled;
      this.authenticator = authenticator;
   }

   public boolean enabled() {
      return enabled;
   }

   public Authenticator authenticator() {
      return authenticator;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }
}
