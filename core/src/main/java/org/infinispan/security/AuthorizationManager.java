package org.infinispan.security;

import java.util.EnumSet;

import javax.security.auth.Subject;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The AuthorizationManager is a cache-scoped component which verifies that the {@link Subject}
 * associated with the current thread, or explicitly specified, has the requested permissions.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface AuthorizationManager {
   /**
    * Verifies that the {@link Subject} associated with the current thread
    * has the requested permission. A {@link SecurityException} is thrown otherwise.
    */
   void checkPermission(AuthorizationPermission permission);

   /**
    * Verifies that the {@link Subject} has the requested permission. A {@link SecurityException} is thrown otherwise.
    */
   void checkPermission(Subject subject, AuthorizationPermission permission);

   /**
    * Verifies that the {@link Subject} associated with the current thread
    * has the requested permission and role. A {@link SecurityException} is thrown otherwise.
    */
   void checkPermission(AuthorizationPermission permission, String role);

   /**
    * Verifies that the {@link Subject} has the requested permission and role.
    * A {@link SecurityException} is thrown otherwise.
    */
   void checkPermission(Subject subject, AuthorizationPermission permission, String role);

   /**
    * Returns the permissions that the specified {@link Subject} has for the cache
    */
   EnumSet<AuthorizationPermission> getPermissions(Subject subject);

   /**
    * Returns the permission required to write to the resource associated with this AuthorizationManager.
    */
   AuthorizationPermission getWritePermission();
}
