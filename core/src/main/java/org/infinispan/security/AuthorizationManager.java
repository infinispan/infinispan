package org.infinispan.security;

import java.security.AccessControlContext;
import java.util.Optional;

import javax.security.auth.Subject;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The AuthorizationManager is a cache-scoped component which verifies that the {@link Subject}
 * associated with the current {@link AccessControlContext} has the requested permissions.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface AuthorizationManager {
   /**
    * Verifies that the {@link Subject} associated with the current {@link AccessControlContext}
    * has the requested permission. A {@link SecurityException} is thrown otherwise.
    *
    * @param permission
    */
   void checkPermission(AuthorizationPermission permission);

   /**
    * Verifies that the {@link Subject} associated with the current {@link AccessControlContext}
    * has the requested permission and role. A {@link SecurityException} is thrown otherwise.
    *
    * @param permission
    * @param requiredRole
    */
   void checkPermission(AuthorizationPermission permission, Optional<String> role);
}
