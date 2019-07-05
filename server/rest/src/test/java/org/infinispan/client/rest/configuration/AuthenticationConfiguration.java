package org.infinispan.client.rest.configuration;

import javax.security.auth.Subject;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class AuthenticationConfiguration {
   private final boolean enabled;
   private final Subject clientSubject;
   private final String mechanism;
   private final String realm;
   private final String username;
   private final char[] password;

   public AuthenticationConfiguration(Subject clientSubject, boolean enabled, String mechanism, String realm, String username, char[] password) {
      this.enabled = enabled;
      this.clientSubject = clientSubject;
      this.mechanism = mechanism;
      this.realm = realm;
      this.username = username;
      this.password = password;
   }

   public boolean enabled() {
      return enabled;
   }

   public String mechanism() {
      return mechanism;
   }

   public Subject clientSubject() {
      return clientSubject;
   }

   public String realm() {
      return realm;
   }

   public String username() {
      return username;
   }

   public char[] password() {
      return password;
   }

}
