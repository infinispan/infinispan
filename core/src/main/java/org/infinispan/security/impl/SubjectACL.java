package org.infinispan.security.impl;

import java.util.Set;

/**
 * SubjectRoles.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class SubjectACL {
   Set<String> roles;

   public SubjectACL(Set<String> roles) {
      this.roles = roles;
   }
}
