package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.AuthenticationConfiguration.CALLBACK_HANDLER;
import static org.infinispan.persistence.remote.configuration.AuthenticationConfiguration.CLIENT_SUBJECT;
import static org.infinispan.persistence.remote.configuration.AuthenticationConfiguration.ENABLED;
import static org.infinispan.persistence.remote.configuration.AuthenticationConfiguration.SASL_PROPERTIES;
import static org.infinispan.persistence.remote.configuration.AuthenticationConfiguration.SERVER_NAME;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class AuthenticationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthenticationConfiguration> {
   private MechanismConfigurationBuilder mechanismConfigurationBuilder;

   public AuthenticationConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, AuthenticationConfiguration.attributeDefinitionSet());
      this.mechanismConfigurationBuilder = new MechanismConfigurationBuilder(builder);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Specifies a {@link CallbackHandler} to be used during the authentication handshake.
    * The {@link Callback}s that need to be handled are specific to the chosen SASL mechanism.
    */
   public AuthenticationConfigurationBuilder callbackHandler(CallbackHandler callbackHandler) {
      this.attributes.attribute(CALLBACK_HANDLER).set(callbackHandler);
      return this;
   }

   /**
    * Configures whether authentication should be enabled or not
    */
   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enables authentication
    */
   public AuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   /**
    * Disables authentication
    */
   public AuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   /**
    * Selects the SASL mechanism to use for the connection to the server
    */
   public AuthenticationConfigurationBuilder saslMechanism(String saslMechanism) {
      mechanismConfigurationBuilder.saslMechanism(saslMechanism);
      return this;
   }

   /**
    * Sets the SASL properties
    */
   public AuthenticationConfigurationBuilder saslProperties(Map<String, String> saslProperties) {
      this.attributes.attribute(SASL_PROPERTIES).set(saslProperties);
      return this;
   }

   /**
    * Sets the name of the server as expected by the SASL protocol
    */
   public AuthenticationConfigurationBuilder serverName(String serverName) {
      this.attributes.attribute(SERVER_NAME).set(serverName);
      return this;
   }

   /**
    * Sets the client subject, necessary for those SASL mechanisms which require it to access client credentials (i.e. GSSAPI)
    */
   public AuthenticationConfigurationBuilder clientSubject(Subject clientSubject) {
      this.attributes.attribute(CLIENT_SUBJECT).set(clientSubject);
      return this;
   }

   /**
    * Specifies the username to be used for authentication. This will use a simple CallbackHandler.
    * This is mutually exclusive with explicitly providing the CallbackHandler
    */
   public AuthenticationConfigurationBuilder username(String username) {
      this.mechanismConfigurationBuilder.username(username);
      return this;
   }

   /**
    * Specifies the password to be used for authentication. A username is also required
    */
   public AuthenticationConfigurationBuilder password(String password) {
      this.mechanismConfigurationBuilder.password(password);
      return this;
   }

   /**
    * Specifies the password to be used for authentication. A username is also required
    */
   public AuthenticationConfigurationBuilder password(char[] password) {
      this.password(new String(password));
      return this;
   }

   /**
    * Specifies the realm to be used for authentication. Username and password also need to be supplied.
    */
   public AuthenticationConfigurationBuilder realm(String realm) {
      this.mechanismConfigurationBuilder.realm(realm);
      return this;
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(attributes.protect(), mechanismConfigurationBuilder.create());
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.mechanismConfigurationBuilder.read(template.mechanismConfiguration(), combine);
      return this;
   }

   @Override
   public void validate() {
      // Delegate validation to the RemoteCacheManager configuration
   }
}
