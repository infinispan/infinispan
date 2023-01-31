package org.infinispan.server.resp.configuration;

import static org.infinispan.server.resp.configuration.RespAuthenticationConfiguration.SECURITY_REALM;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.AbstractProtocolServerConfigurationChildBuilder;
import org.infinispan.server.core.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.resp.logging.Log;


/**
 * RespAuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
public class RespAuthenticationConfigurationBuilder extends AbstractProtocolServerConfigurationChildBuilder<RespServerConfiguration, RespAuthenticationConfigurationBuilder, RespAuthenticationConfiguration> implements AuthenticationConfigurationBuilder<RespAuthenticationConfiguration> {
   private final AttributeSet attributes;
   private UsernamePasswordAuthenticator authenticator;
   private boolean enabled;

   RespAuthenticationConfigurationBuilder(ProtocolServerConfigurationBuilder builder) {
      super(builder);
      attributes = RespAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RespAuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   public RespAuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   public RespAuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public RespAuthenticationConfigurationBuilder securityRealm(String realm) {
      attributes.attribute(SECURITY_REALM).set(realm);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(SECURITY_REALM).isNull();
   }

   public RespAuthenticationConfigurationBuilder authenticator(UsernamePasswordAuthenticator authenticator) {
      this.authenticator = authenticator;
      return this.enable();
   }

   @Override
   public void validate() {
      if (enabled && authenticator == null) {
         throw Log.CONFIG.authenticationWithoutAuthenticator();
      }
   }

   @Override
   public RespAuthenticationConfiguration create() {
      return new RespAuthenticationConfiguration(attributes.protect(), authenticator, enabled);
   }

   @Override
   public Builder<?> read(RespAuthenticationConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }


   @Override
   public RespAuthenticationConfigurationBuilder self() {
      return this;
   }
}
