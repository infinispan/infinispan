package org.infinispan.query;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAuthorizationManagerAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;

import java.security.AccessController;
import java.security.PrivilegedAction;

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
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static AuthorizationManager getCacheAuthorizationManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheAuthorizationManagerAction(cache));
   }

   static Configuration getCacheConfiguration(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheConfigurationAction(cache));
   }
}
