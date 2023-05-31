package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.RestAuthenticationConfiguration.MECHANISMS;
import static org.infinispan.rest.configuration.RestAuthenticationConfiguration.METRICS_AUTH;
import static org.infinispan.rest.configuration.RestAuthenticationConfiguration.SECURITY_REALM;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.rest.authentication.RestAuthenticator;
import org.infinispan.server.core.configuration.AbstractProtocolServerConfigurationChildBuilder;
import org.infinispan.server.core.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;


/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class RestAuthenticationConfigurationBuilder extends AbstractProtocolServerConfigurationChildBuilder<RestServerConfiguration, RestAuthenticationConfigurationBuilder, RestAuthenticationConfiguration> implements AuthenticationConfigurationBuilder<RestAuthenticationConfiguration> {
   private final AttributeSet attributes;
   private RestAuthenticator authenticator;
   private boolean enabled;

   RestAuthenticationConfigurationBuilder(ProtocolServerConfigurationBuilder builder) {
      super(builder);
      attributes = RestAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RestAuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   public RestAuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   public RestAuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public boolean enabled() {
      return enabled;
   }

   public RestAuthenticationConfigurationBuilder securityRealm(String realm) {
      attributes.attribute(SECURITY_REALM).set(realm);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(SECURITY_REALM).isNull();
   }

   public RestAuthenticationConfigurationBuilder authenticator(RestAuthenticator authenticator) {
      this.authenticator = authenticator;
      return this.enable();
   }

   public RestAuthenticationConfigurationBuilder addMechanisms(String... mechanisms) {
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

   public RestAuthenticationConfigurationBuilder metricsAuth(boolean metricsAuth) {
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
   public RestAuthenticationConfiguration create() {
      return new RestAuthenticationConfiguration(attributes.protect(), authenticator, enabled);
   }

   @Override
   public Builder<?> read(RestAuthenticationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public RestAuthenticationConfigurationBuilder self() {
      return this;
   }
}
