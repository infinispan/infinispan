package org.infinispan.query.backend;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAction;

import java.security.AccessController;

/**
 * SecurityActions for the org.infinispan.query.backend package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
final class SecurityActions {

   @SuppressWarnings("unchecked")
   static <K, V> Cache<K, V> getCache(EmbeddedCacheManager cacheManager, String cacheName) {
      GetCacheAction action = new GetCacheAction(cacheManager, cacheName);
      if (System.getSecurityManager() != null) {
         return (Cache<K, V>) AccessController.doPrivileged(action);
      } else {
         return (Cache<K, V>) Security.doPrivileged(action);
      }
   }
}
