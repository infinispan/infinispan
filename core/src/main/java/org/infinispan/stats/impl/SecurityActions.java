package org.infinispan.stats.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetClusterExecutorAction;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * SecurityActions for the org.infinispan.stats.impl package.
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

   static ClusterExecutor getClusterExecutor(final Cache<?, ?> cache) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cache);
      return doPrivileged(action);
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }

   static <K, V> Cache<K, V> getUnwrappedCache(final Cache<K, V> cache) {
      if (cache instanceof SecureCacheImpl) {
         return doPrivileged(() ->  ((SecureCacheImpl)cache).getDelegate() );
      } else {
         return cache;
      }
   }
}
