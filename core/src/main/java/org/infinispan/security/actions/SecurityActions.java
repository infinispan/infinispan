package org.infinispan.security.actions;

import static org.infinispan.security.Security.doPrivileged;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.Health;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listenable;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * @since 15.0
 **/
public class SecurityActions {

   public static EmbeddedCacheManager getEmbeddedCacheManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetEmbeddedCacheManagerAction(cache));
   }
   public static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(cacheManager::getGlobalComponentRegistry);
   }

   public static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager cacheManager) {
      return doPrivileged(cacheManager::getCacheManagerConfiguration);
   }

   public static <A extends Cache<K,V>, K, V> A getUnwrappedCache(Cache<K, V> cache) {
      return doPrivileged(new GetUnwrappedCacheAction<>(cache));
   }

   public static <A extends Cache<K,V>, K, V> A getUnwrappedCache(EmbeddedCacheManager cacheManager, String cacheName) {
      return doPrivileged(new GetUnwrappedNameCacheAction<>(cacheManager, cacheName));
   }

   public static void defineConfiguration(final EmbeddedCacheManager cacheManager, final String cacheName, final Configuration configurationOverride) {
      doPrivileged(new DefineConfigurationAction(cacheManager, cacheName, configurationOverride));
   }

   public static <A extends Cache<K,V>, K, V> A getCache(final EmbeddedCacheManager cacheManager, final String cacheName) {
      return doPrivileged(new GetCacheAction<>(cacheManager, cacheName));
   }

   public static Configuration getCacheConfiguration(EmbeddedCacheManager cacheManager, String name) {
      return doPrivileged(new GetCacheConfigurationFromManagerAction(cacheManager, name));
   }

   public static void stopCache(Cache<?, ?> cache) {
      doPrivileged(() -> {
         cache.stop();
         return null;
      });
   }

   public static ClusterExecutor getClusterExecutor(final Cache<?, ?> cache) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cache);
      return doPrivileged(action);
   }

   public static ClusterExecutor getClusterExecutor(final EmbeddedCacheManager cacheManager) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cacheManager);
      return doPrivileged(action);
   }

   public static void checkPermission(EmbeddedCacheManager cacheManager, AuthorizationPermission permission) {
      Authorizer authorizer = getGlobalComponentRegistry(cacheManager).getComponent(Authorizer.class);
      authorizer.checkPermission(permission);
   }

   public static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> advancedCache) {
      return doPrivileged(advancedCache::getComponentRegistry);
   }

   public static void undefineConfiguration(EmbeddedCacheManager cacheManager, String name) {
      UndefineConfigurationAction action = new UndefineConfigurationAction(cacheManager, name);
      doPrivileged(action);
   }

   public static AuthorizationManager getCacheAuthorizationManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getAuthorizationManager);
   }

   public static void addListener(EmbeddedCacheManager cacheManager, Object listener) {
      doPrivileged(new AddCacheManagerListenerAction(cacheManager, listener));
   }

   public static CompletionStage<Void> removeListenerAsync(Listenable listenable, Object listener) {
      RemoveListenerAsyncAction action = new RemoveListenerAsyncAction(listenable, listener);
      return doPrivileged(action);
   }

   public static <K, V> CompletionStage<CacheEntry<K, V>> getCacheEntryAsync(final AdvancedCache<K, V> cache, K key) {
      GetCacheEntryAsyncAction<K, V> action = new GetCacheEntryAsyncAction<>(cache, key);
      return doPrivileged(action);
   }

   public static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getCacheConfiguration);
   }

   public static <K> CompletionStage<Boolean> cacheContainsKeyAsync(AdvancedCache<K, ?> ac, K key) {
      return doPrivileged(new CacheContainsKeyAsyncAction<>(ac, key));
   }

   public static void addCacheDependency(EmbeddedCacheManager cacheManager, String from, String to) {
      doPrivileged(new AddCacheDependencyAction(cacheManager, from, to));
   }

   public static PersistenceManager getPersistenceManager(final EmbeddedCacheManager cacheManager, String cacheName) {
      final GetPersistenceManagerAction action = new GetPersistenceManagerAction(cacheManager, cacheName);
      return doPrivileged(action);
   }

   public static Health getHealth(final EmbeddedCacheManager cacheManager) {
      GetCacheManagerHealthAction action = new GetCacheManagerHealthAction(cacheManager);
      return doPrivileged(action);
   }

   public static CompletionStage<Void> addLoggerListenerAsync(EmbeddedCacheManager ecm, Object listener) {
      return doPrivileged(new AddLoggerListenerAsyncAction(ecm, listener));
   }

   public static CompletionStage<Void> addListenerAsync(EmbeddedCacheManager cacheManager, Object listener) {
      return doPrivileged(new AddCacheManagerListenerAsyncAction(cacheManager, listener));
   }

   public static CacheEntry<String, String> getCacheEntry(AdvancedCache<String, String> cache, String key) {
      return doPrivileged(new GetCacheEntryAction<>(cache, key));
   }

   public static DistributionManager getDistributionManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getDistributionManager);
   }

   public static <K, V> AdvancedCache<K, V> anonymizeSecureCache(AdvancedCache<K, V> cache) {
      return doPrivileged(() -> cache.transform(SecurityActions::unsetSubject));
   }

   private static <K, V> AdvancedCache<K, V> unsetSubject(AdvancedCache<K, V> cache) {
      if (cache instanceof SecureCacheImpl) {
         return new SecureCacheImpl<>(getUnwrappedCache(cache));
      } else {
         return cache;
      }
   }

   public static <A extends Cache<K, V>, K, V> A getOrCreateCache(EmbeddedCacheManager cm, String configName, Configuration cfg) {
      GetOrCreateCacheAction<A, K, V> action = new GetOrCreateCacheAction<>(cm, configName, cfg);
      return doPrivileged(action);
   }

   public static Configuration getOrCreateTemplate(EmbeddedCacheManager cm, String configName, Configuration cfg) {
      GetOrCreateTemplateAction action = new GetOrCreateTemplateAction(cm, configName, cfg);
      return doPrivileged(action);
   }

   public static void stopManager(EmbeddedCacheManager cacheManager) {
      doPrivileged(() -> {
         cacheManager.stop();
         return null;
      });
   }
}
