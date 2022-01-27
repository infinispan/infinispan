package org.infinispan.server.resp.configuration;

import static org.infinispan.server.resp.configuration.AuthenticationConfiguration.SECURITY_REALM;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.AbstractProtocolServerConfigurationChildBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.resp.Authenticator;


/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
public class AuthenticationConfigurationBuilder extends AbstractProtocolServerConfigurationChildBuilder<RespServerConfiguration, AuthenticationConfigurationBuilder> implements Builder<AuthenticationConfiguration> {
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

   @Override
   public void validate() {
      if (enabled && authenticator == null) {
         throw RespServerConfigurationBuilder.logger.authenticationWithoutAuthenticator();
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
