package org.infinispan.security;

import java.security.AccessControlContext;

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
   void checkPermission(AuthorizationPermission permissions);
}