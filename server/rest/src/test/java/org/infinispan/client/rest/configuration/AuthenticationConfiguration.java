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

   public AuthenticationConfiguration(Subject clientSubject, boolean enabled, String mechanism) {
      this.enabled = enabled;
      this.clientSubject = clientSubject;
      this.mechanism = mechanism;
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

}
