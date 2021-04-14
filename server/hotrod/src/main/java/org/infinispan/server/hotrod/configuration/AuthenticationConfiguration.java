package org.infinispan.server.hotrod.configuration;

import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.security.ServerAuthenticationProvider;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfiguration {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder(Attribute.SECURITY_REALM, null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, SECURITY_REALM);
   }

   private final AttributeSet attributes;
   private final boolean enabled;
   private final SaslConfiguration saslConfiguration;
   private final ServerAuthenticationProvider serverAuthenticationProvider;
   private final Subject serverSubject;

   AuthenticationConfiguration(AttributeSet attributes, SaslConfiguration saslConfiguration, boolean enabled, ServerAuthenticationProvider serverAuthenticationProvider, Subject serverSubject) {
      this.attributes = attributes.checkProtection();
      this.saslConfiguration = saslConfiguration;
      this.enabled = enabled;
      this.serverAuthenticationProvider = serverAuthenticationProvider;
      this.serverSubject = serverSubject;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return enabled;
   }

   public Set<String> allowedMechs() {
      return saslConfiguration.mechanisms();
   }

   public ServerAuthenticationProvider serverAuthenticationProvider() {
      return serverAuthenticationProvider;
   }

   public Map<String, String> mechProperties() {
      return saslConfiguration.mechProperties();
   }

   public String serverName() {
      return saslConfiguration.serverName();
   }

   public Subject serverSubject() {
      return serverSubject;
   }

   public SaslConfiguration sasl() {
      return saslConfiguration;
   }

   @Override
   public String toString() {
      return "AuthenticationConfiguration{" +
            "attributes=" + attributes +
            ", enabled=" + enabled +
            ", saslConfiguration=" + saslConfiguration +
            ", serverAuthenticationProvider=" + serverAuthenticationProvider +
            ", serverSubject=" + serverSubject +
            '}';
   }
}
