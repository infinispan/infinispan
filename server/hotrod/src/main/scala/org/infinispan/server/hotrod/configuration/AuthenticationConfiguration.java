package org.infinispan.server.hotrod.configuration;

import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.server.core.security.ServerAuthenticationProvider;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfiguration {
   private final boolean enabled;
   private final Set<String> allowedMechs;
   private final ServerAuthenticationProvider serverAuthenticationProvider;
   private final Map<String, String> mechProperties;
   private final String serverName;
   private final Subject serverSubject;

   AuthenticationConfiguration(boolean enabled, Set<String> set, ServerAuthenticationProvider serverAuthenticationProvider,
         Map<String, String> mechProperties, String serverName, Subject serverSubject) {
      this.enabled = enabled;
      this.allowedMechs = set;
      this.serverAuthenticationProvider = serverAuthenticationProvider;
      this.mechProperties = mechProperties;
      this.serverName = serverName;
      this.serverSubject = serverSubject;
   }

   public boolean enabled() {
      return enabled;
   }

   public Set<String> allowedMechs() {
      return allowedMechs;
   }

   public ServerAuthenticationProvider serverAuthenticationProvider() {
      return serverAuthenticationProvider;
   }

   public Map<String, String> mechProperties() {
      return mechProperties;
   }

   public String serverName() {
      return serverName;
   }

   public Subject serverSubject() {
      return serverSubject;
   }

   @Override
   public String toString() {
      return "AuthenticationConfiguration [enabled=" + enabled + ", allowedMechs=" + allowedMechs
            + ", serverAuthenticationProvider=" + serverAuthenticationProvider + ", mechProperties=" + mechProperties
            + ", serverName=" + serverName + ", serverSubject=" + serverSubject + "]";
   }
}
