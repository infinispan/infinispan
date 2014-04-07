package org.infinispan.security;

import java.security.Permission;
import java.security.PermissionCollection;

/**
 * SecurityPermission.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SecurityPermission extends Permission {
   private static final long serialVersionUID = 9120524408585262253L;
   private final AuthorizationPermission authzPermission;

   public SecurityPermission(String name) {
      this(AuthorizationPermission.valueOf(name));
   }

   public SecurityPermission(AuthorizationPermission perm) {
      super(perm.toString());
      authzPermission = perm;
   }

   @Override
   public boolean implies(Permission permission) {
      if ((permission == null) || (permission.getClass() != getClass()))
         return false;
      SecurityPermission that = (SecurityPermission) permission;
      return this.authzPermission.implies(that.authzPermission);
   }

   @Override
   public boolean equals(Object obj) {
      if (obj == this)
         return true;

     if ((obj == null) || (obj.getClass() != getClass()))
         return false;

     SecurityPermission that = (SecurityPermission) obj;

     return this.authzPermission == that.authzPermission;
   }

   @Override
   public int hashCode() {
      return authzPermission.hashCode();
   }

   @Override
   public String getActions() {
      return "";
   }

   public PermissionCollection newPermissionCollection() {
      return new SecurityPermissionCollection();
   }

   public AuthorizationPermission getAuthorizationPermission() {
      return authzPermission;
   }
}
