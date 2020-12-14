package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.AuthenticationConfiguration.MECHANISMS;
import static org.infinispan.rest.configuration.AuthenticationConfiguration.METRICS_AUTH;
import static org.infinispan.rest.configuration.AuthenticationConfiguration.SECURITY_REALM;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.core.configuration.AbstractProtocolServerConfigurationChildBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;


/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class AuthenticationConfigurationBuilder extends AbstractProtocolServerConfigurationChildBuilder<RestServerConfiguration, AuthenticationConfigurationBuilder> implements Builder<AuthenticationConfiguration> {
   private final AttributeSet attributes;
   private Authenticator authenticator;
   private boolean enabled;

   AuthenticationConfigurationBuilder(ProtocolServerConfigurationBuilder builder) {
      super(builder);
      attributes = AuthenticationConfiguration.attributeDefinitionSet();
   }

   public AuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   public AuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public AuthenticationConfigurationBuilder securityRealm(String realm) {
      attributes.attribute(SECURITY_REALM).set(realm);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(SECURITY_REALM).isNull();
   }

   public AuthenticationConfigurationBuilder authenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
      return this.enable();
   }

   public AuthenticationConfigurationBuilder addMechanisms(String... mechanisms) {
      List<String> mechs = attributes.attribute(MECHANISMS).get();
      for (int i = 0; i < mechanisms.length; i++) {
         mechs.add(mechanisms[i]);
      }
      attributes.attribute(MECHANISMS).set(mechs);
      return this.enable();
   }

   public boolean hasMechanisms() {
      return !attributes.attribute(MECHANISMS).get().isEmpty();
   }

   public List<String> mechanisms() {
      return attributes.attribute(MECHANISMS).get();
   }

   public AuthenticationConfigurationBuilder metricsAuth(boolean metricsAuth) {
      attributes.attribute(METRICS_AUTH).set(metricsAuth);
      return this;
   }

   @Override
   public void validate() {
      if (enabled && authenticator == null) {
         throw RestServerConfigurationBuilder.logger.authenticationWithoutAuthenticator();
      }
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(attributes.protect(), authenticator, enabled);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }


   @Override
   public AuthenticationConfigurationBuilder self() {
      return this;
   }
}
