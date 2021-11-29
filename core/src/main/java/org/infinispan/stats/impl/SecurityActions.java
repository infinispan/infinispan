package org.infinispan.stats.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
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

   static ClusterExecutor getClusterExecutor(final EmbeddedCacheManager cacheManager) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cacheManager);
      return doPrivileged(action);
   }

   static <K, V> Cache<K, V> getUnwrappedCache(final Cache<K, V> cache) {
      if (cache instanceof SecureCacheImpl) {
         return doPrivileged(((SecureCacheImpl<K, V>) cache)::getDelegate);
      } else {
         return cache;
      }
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(cacheManager));
   }

   public static CacheMgmtInterceptor getCacheMgmtInterceptor(AdvancedCache<?, ?> cache) {
      ComponentRegistry componentRegistry = doPrivileged(new GetCacheComponentRegistryAction(cache));
      return componentRegistry.getComponent(CacheMgmtInterceptor.class);
   }
}
