package org.infinispan.client.hotrod.configuration;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfiguration {
   private final boolean enabled;
   private final CallbackHandler callbackHandler;
   private final Subject clientSubject;
   private final String saslMechanism;
   private final Map<String, String> saslProperties;
   private final String serverName;

   public AuthenticationConfiguration(CallbackHandler callbackHandler, Subject clientSubject, boolean enabled, String saslMechanism, Map<String, String> saslProperties, String serverName) {
      this.enabled = enabled;
      this.callbackHandler = callbackHandler;
      this.clientSubject = clientSubject;
      this.saslMechanism = saslMechanism;
      this.saslProperties = saslProperties;
      this.serverName = serverName;
   }

   public CallbackHandler callbackHandler() {
      return callbackHandler;
   }

   public boolean enabled() {
      return enabled;
   }

   public String saslMechanism() {
      return saslMechanism;
   }

   public Map<String, String> saslProperties() {
      return saslProperties;
   }

   public String serverName() {
      return serverName;
   }

   public Subject clientSubject() {
      return clientSubject;
   }

}
