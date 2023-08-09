package org.infinispan.security.impl;

import java.util.EnumSet;

import javax.security.auth.Subject;

import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * A permissive {@link AuthorizationManager}.
 * @since 14.0
 **/
public class PermissiveAuthorizationManager implements AuthorizationManager {
   @Override
   public void checkPermission(AuthorizationPermission permission) {
   }

   @Override
   public void checkPermission(Subject subject, AuthorizationPermission permission) {
   }

   @Override
   public void checkPermission(AuthorizationPermission permission, String role) {
   }

   @Override
   public void checkPermission(Subject subject, AuthorizationPermission permission, String role) {
   }

   @Override
   public EnumSet<AuthorizationPermission> getPermissions(Subject subject) {
      return EnumSet.allOf(AuthorizationPermission.class);
   }

   @Override
   public AuthorizationPermission getWritePermission() {
      return AuthorizationPermission.CREATE;
   }

   @Override
   public void doIf(Subject subject, AuthorizationPermission permission, Runnable runnable) {
      runnable.run();
   }

   @Override
   public boolean isPermissive() {
      return true;
   }
}
