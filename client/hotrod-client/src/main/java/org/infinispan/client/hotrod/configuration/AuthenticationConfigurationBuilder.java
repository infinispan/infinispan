package org.infinispan.client.hotrod.configuration;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthenticationConfiguration> {
   private static final Log log = LogFactory.getLog(AuthenticationConfigurationBuilder.class);
   private CallbackHandler callbackHandler;
   private boolean enabled = false;
   private String serverName;
   private Map<String, String> saslProperties;
   private String saslMechanism;
   private Subject clientSubject;

   public AuthenticationConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder);
   }

   public AuthenticationConfigurationBuilder callbackHandler(CallbackHandler callbackHandler) {
      this.callbackHandler = callbackHandler;
      return this;
   }

   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public AuthenticationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public AuthenticationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public AuthenticationConfigurationBuilder saslMechanism(String saslMechanism) {
      this.saslMechanism = saslMechanism;
      return this;
   }

   public AuthenticationConfigurationBuilder serverName(String serverName) {
      this.serverName = serverName;
      return this;
   }

   public AuthenticationConfigurationBuilder clientSubject(Subject clientSubject) {
      this.clientSubject = clientSubject;
      return this;
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(callbackHandler, clientSubject, enabled, saslMechanism, saslProperties, serverName);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template) {
      this.callbackHandler = template.callbackHandler();
      this.clientSubject = template.clientSubject();
      this.enabled = template.enabled();
      this.saslMechanism = template.saslMechanism();
      this.saslProperties = template.saslProperties();
      this.serverName = template.serverName();
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (callbackHandler == null && clientSubject == null) {
            throw log.invalidCallbackHandler();
         }
         if (saslMechanism == null) {
            throw log.invalidSaslMechanism(saslMechanism);
         }
      }
   }

}
