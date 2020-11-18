package org.infinispan.rest.resources.security;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.rest.authentication.SecurityDomain;

/**
 * Security domain that supports a simple map of subjects
 */
public class SimpleSecurityDomain implements SecurityDomain {

   private final Map<String, Subject> subjects;

   public SimpleSecurityDomain(Subject... subjects) {
      this.subjects = new HashMap<>(subjects.length);
      for (Subject subject : subjects) {
         this.subjects.put(subject.getPrincipals().iterator().next().getName().toLowerCase(), subject);
      }
   }

   @Override
   public Subject authenticate(String username, String password) throws SecurityException {
      if (username.equals(password)) {
         return subjects.get(username);
      }
      return null;
   }
}
