package org.infinispan.query.core.stats.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAction;
import org.infinispan.security.actions.GetClusterExecutorAction;

/**
 * SecurityActions for the org.infinispan.query.core.impl.stats package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @since 12.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static Cache<?, ?> getCache(final EmbeddedCacheManager cacheManager, final String cacheName) {
      return doPrivileged(new GetCacheAction(cacheManager, cacheName));
   }

   static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getComponentRegistry);
   }

   public static SearchStatsRetriever getStatsRetriever(Cache<?, ?> cache) {
      return getCacheComponentRegistry(cache.getAdvancedCache()).getComponent(SearchStatsRetriever.class);
   }

   static ClusterExecutor getClusterExecutor(final Cache<?, ?> cache) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cache);
      return doPrivileged(action);
   }

}
