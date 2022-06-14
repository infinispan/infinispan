package org.infinispan.rest.distribution;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAction;

/**
 * SecurityActions for the org.infinispan.rest.distribution package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @since 14.0
 */
public final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static Cache<?, ?> getCache(String cacheName, EmbeddedCacheManager manager) {
      GetCacheAction action = new GetCacheAction(manager, cacheName);
      return doPrivileged(action);
   }
}
