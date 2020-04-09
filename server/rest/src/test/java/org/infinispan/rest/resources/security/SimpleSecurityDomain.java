package org.infinispan.rest.resources.security;

import javax.security.auth.Subject;

import org.infinispan.rest.authentication.SecurityDomain;

/**
 * Security domain that returns always the same subject
 */
public class SimpleSecurityDomain implements SecurityDomain {

   private final Subject subject;

   public SimpleSecurityDomain(Subject subject) {
      this.subject = subject;
   }

   @Override
   public Subject authenticate(String username, String password) throws SecurityException {
      if (username.equals(password)) {
         return subject;
      }
      return null;
   }
}
