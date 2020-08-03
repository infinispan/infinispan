package org.infinispan.server.hotrod.configuration;

import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.hotrod.logging.Log;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfigurationBuilder extends AbstractHotRodServerChildConfigurationBuilder implements Builder<AuthenticationConfiguration> {
   private final AttributeSet attributes;

   private static final Log log = LogFactory.getLog(AuthenticationConfigurationBuilder.class, Log.class);
   private boolean enabled = false;
   private ServerAuthenticationProvider serverAuthenticationProvider;
   private Subject serverSubject;
   private SaslConfigurationBuilder sasl = new SaslConfigurationBuilder();

   AuthenticationConfigurationBuilder(HotRodServerChildConfigurationBuilder builder) {
      super(builder);
      this.attributes = AuthenticationConfiguration.attributeDefinitionSet();
   }

   public AuthenticationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public AuthenticationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public AuthenticationConfigurationBuilder serverAuthenticationProvider(ServerAuthenticationProvider serverAuthenticationProvider) {
      this.serverAuthenticationProvider = serverAuthenticationProvider;
      return this;
   }

   public AuthenticationConfigurationBuilder addMechanisms(String... mechs) {
      sasl.addMechanisms(mechs);
      return this;
   }

   public boolean hasMechanisms() {
      return sasl.hasMechanisms();
   }

   public AuthenticationConfigurationBuilder addAllowedMech(String mech) {
      sasl.addAllowedMech(mech);
      return this;
   }

   public AuthenticationConfigurationBuilder mechProperties(Map<String, String> mechProperties) {
      sasl.setMechProperty(mechProperties);
      return this;
   }

   public AuthenticationConfigurationBuilder addMechProperty(String key, String value) {
      sasl.addMechProperty(key, value);
      return this;
   }

   public AuthenticationConfigurationBuilder serverName(String serverName) {
      sasl.serverName(serverName);
      return this;
   }

   public AuthenticationConfigurationBuilder serverSubject(Subject serverSubject) {
      this.serverSubject = serverSubject;
      return this;
   }

   public AuthenticationConfigurationBuilder securityRealm(String name) {
      attributes.attribute(AuthenticationConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(AuthenticationConfiguration.SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(AuthenticationConfiguration.SECURITY_REALM).isNull();
   }

   public SaslConfigurationBuilder sasl() {
      return sasl;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (serverAuthenticationProvider == null) {
            throw log.serverAuthenticationProvider();
         }
         sasl.validate();
      }
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(attributes.protect(), sasl.create(), enabled, serverAuthenticationProvider, serverSubject);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template) {
      this.enabled = template.enabled();
      this.serverAuthenticationProvider = template.serverAuthenticationProvider();
      this.sasl.read(template.sasl());
      return this;
   }
}
