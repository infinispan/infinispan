package org.infinispan.server.core.backup.resources;

/**
 * SecurityActions for the org.infinispan.server.core.backup.resources package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheConfigurationFromManagerAction;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.actions.GetOrCreateCacheAction;
import org.infinispan.security.actions.GetOrCreateTemplateAction;
import org.infinispan.security.actions.GetUnwrappedCacheAction;
import org.infinispan.security.impl.Authorizer;

final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static GlobalConfiguration getGlobalConfiguration(final EmbeddedCacheManager cacheManager) {
      GetCacheManagerConfigurationAction action = new GetCacheManagerConfigurationAction(cacheManager);
      return doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
      return doPrivileged(action);
   }

   static void checkPermission(EmbeddedCacheManager cacheManager, AuthorizationPermission permission) {
      Authorizer authorizer = getGlobalComponentRegistry(cacheManager).getComponent(Authorizer.class);
      authorizer.checkPermission(cacheManager.getSubject(), permission);
   }

   static Configuration getCacheConfiguration(EmbeddedCacheManager cm, String cacheName) {
      GetCacheConfigurationFromManagerAction action = new GetCacheConfigurationFromManagerAction(cm, cacheName);
      return doPrivileged(action);
   }

   static <K, V> AdvancedCache<K, V> getUnwrappedCache(Cache<K, V> cache) {
      GetUnwrappedCacheAction<AdvancedCache<K, V>, K, V> action = new GetUnwrappedCacheAction(cache);
      return doPrivileged(action);
   }

   static Cache<?, ?> getOrCreateCache(EmbeddedCacheManager cm, String cacheName, Configuration cfg) {
      GetOrCreateCacheAction action = new GetOrCreateCacheAction(cm, cacheName, cfg);
      return doPrivileged(action);
   }

   static Configuration getOrCreateTemplate(EmbeddedCacheManager cm, String configName, Configuration cfg) {
      GetOrCreateTemplateAction action = new GetOrCreateTemplateAction(cm, configName, cfg);
      return doPrivileged(action);
   }

   static ComponentRegistry getComponentRegistry(AdvancedCache<?,?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      return doPrivileged(action);
   }
}
