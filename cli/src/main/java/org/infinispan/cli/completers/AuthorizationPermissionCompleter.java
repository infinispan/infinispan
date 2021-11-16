package org.infinispan.cli.completers;

/**
 * @since 14.0
 */
public class AuthorizationPermissionCompleter extends EnumCompleter<AuthorizationPermissionCompleter.AuthorizationPermission> {
   public enum AuthorizationPermission {
      LIFECYCLE,
      READ,
      WRITE,
      EXEC,
      LISTEN,
      BULK_READ,
      BULK_WRITE,
      ADMIN,
      CREATE,
      MONITOR,
      ALL,
      ALL_READ,
      ALL_WRITE,
   }

   public AuthorizationPermissionCompleter() {
      super(AuthorizationPermissionCompleter.AuthorizationPermission.class);
   }
}
