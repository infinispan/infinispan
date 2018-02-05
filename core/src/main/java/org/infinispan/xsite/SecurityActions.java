package org.infinispan.xsite;

import java.security.AccessController;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.security.Security;
import org.infinispan.security.actions.ContextAwarePrivilegedAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.UnwrapCacheAction;

/**
 * SecurityActions for the org.infinispan.xsite package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author vjuranek
 * @since 9.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(ContextAwarePrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else if (action.contextRequiresSecurity()) {
         return Security.doPrivileged(action);
      } else {
         return action.run();
      }
   }

   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      return doPrivileged(action);
   }

   static <K, V> AdvancedCache<K, V> getUnwrappedCache(final Cache<K, V> cache) {
      UnwrapCacheAction<K, V> action = new UnwrapCacheAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }
}
