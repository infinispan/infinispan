package org.infinispan.registry.impl;

import java.security.AccessController;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.DefineConfigurationAction;
import org.infinispan.security.actions.GetCacheAction;

/**
 * SecurityActions for the org.infinispan.registry.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
   static void defineConfiguration(final EmbeddedCacheManager cacheManager, final String cacheName, final Configuration configurationOverride) {
      DefineConfigurationAction action = new DefineConfigurationAction(cacheManager, cacheName, configurationOverride);
      if (System.getSecurityManager() != null) {
         AccessController.doPrivileged(action);
      } else {
         Security.doPrivileged(action);
      }
   }

   @SuppressWarnings("unchecked")
   static <K, V> Cache<K, V> getRegistryCache(EmbeddedCacheManager cacheManager) {
      GetCacheAction action = new GetCacheAction(cacheManager, ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME);
      if (System.getSecurityManager() != null) {
         return (Cache<K, V>) AccessController.doPrivileged(action);
      } else {
         return (Cache<K, V>) Security.doPrivileged(action);
      }
   }
}
