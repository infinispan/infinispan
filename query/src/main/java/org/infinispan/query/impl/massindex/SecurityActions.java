package org.infinispan.query.impl.massindex;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * SecurityActions for the org.infinispan.query.impl.massindex package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 * @since 9.1
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static <K, V> Cache<K, V> getUnwrappedCache(final Cache<K, V> cache) {
      if (cache instanceof SecureCacheImpl) {
         return doPrivileged(() -> ((SecureCacheImpl) cache).getDelegate());
      } else {
         return cache;
      }
   }
}
