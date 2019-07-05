package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.AuthenticationConfiguration.AUTHENTICATOR;
import static org.infinispan.rest.configuration.AuthenticationConfiguration.ENABLED;
import static org.infinispan.rest.configuration.AuthenticationConfiguration.MECHANISMS;

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
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public AuthenticationConfigurationBuilder authenticator(Authenticator authenticator) {
      attributes.attribute(AUTHENTICATOR).set(authenticator);
      return this.enable();
   }

   public AuthenticationConfigurationBuilder addMechanisms(String... mechanisms) {
      List<String> mechs = attributes.attribute(MECHANISMS).get();
      for(int i = 0; i < mechanisms.length; i++) {
         mechs.add(mechanisms[i]);
      }
      attributes.attribute(MECHANISMS).set(mechs);
      return this.enable();
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && attributes.attribute(AUTHENTICATOR).isNull()) {
         throw RestServerConfigurationBuilder.logger.authenticationWithoutAuthenticator();
      }
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(attributes.protect());
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
