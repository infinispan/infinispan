package org.infinispan.rest.cachemanager;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listenable;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.AddCacheManagerListenerAction;
import org.infinispan.security.actions.GetCacheAuthorizationManagerAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetCacheDistributionManagerAction;
import org.infinispan.security.actions.GetCacheEntryAsyncAction;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetClusterExecutorAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.actions.RemoveListenerAsyncAction;

/**
 * SecurityActions for the org.infinispan.rest.cachemanager package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void addListener(EmbeddedCacheManager cacheManager, Object listener) {
      doPrivileged(new AddCacheManagerListenerAction(cacheManager, listener));
   }

   static CompletionStage<Void> removeListenerAsync(Listenable listenable, Object listener) {
      RemoveListenerAsyncAction action = new RemoveListenerAsyncAction(listenable, listener);
      return doPrivileged(action);
   }

   static DistributionManager getDistributionManager(final Cache<?, ?> cache) {
      GetCacheDistributionManagerAction action = new GetCacheDistributionManagerAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }

   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      return doPrivileged(action);
   }

   static <K, V> CompletionStage<CacheEntry<K, V>> getCacheEntryAsync(final AdvancedCache<K, V> cache, K key) {
      GetCacheEntryAsyncAction<K, V> action = new GetCacheEntryAsyncAction<>(cache, key);
      return doPrivileged(action);
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(cacheManager));
   }

   static ClusterExecutor getClusterExecutor(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetClusterExecutorAction(cacheManager));
   }

   static AuthorizationManager getCacheAuthorizationManager(final AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheAuthorizationManagerAction(cache));
   }
}
