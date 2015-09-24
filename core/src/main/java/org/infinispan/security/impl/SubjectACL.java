package org.infinispan.security.impl;

import java.util.Collections;
import java.util.Set;

/**
 * SubjectACL.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class SubjectACL {
   private final Set<String> roles;
   private final int mask;

   public SubjectACL(Set<String> roles, int mask) {
      this.roles = Collections.unmodifiableSet(roles);
      this.mask = mask;
   }

   public int getMask() {
      return mask;
   }

   public Set<String> getRoles() {
      return roles;
   }

   public boolean containsRole(String role) {
      return roles.contains(role);
   }

   public boolean matches(int permissionMask) {
      return (mask & permissionMask) == permissionMask;
   }

   @Override
   public String toString() {
      return "SubjectACL [roles=" + roles + ", mask=" + mask + "]";
   }
}
