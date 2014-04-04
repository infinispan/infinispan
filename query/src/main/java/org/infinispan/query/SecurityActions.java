package org.infinispan.query;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.security.AuthorizationManager;

/**
 * SecurityActions for the org.infinispan.query package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
   static AuthorizationManager getCacheAuthorizationManager(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<AuthorizationManager>() {
            @Override
            public AuthorizationManager run() {
               return cache.getAuthorizationManager();
            }
         });
      } else {
         return cache.getAuthorizationManager();
      }
   }
}
