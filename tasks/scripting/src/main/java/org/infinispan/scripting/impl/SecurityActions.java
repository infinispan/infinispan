package org.infinispan.scripting.impl;

import java.security.AccessController;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.ContextAwarePrivilegedAction;
import org.infinispan.security.actions.GetCacheAuthorizationManagerAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetCacheEntryAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.actions.UnwrapCacheAction;

/**
 * SecurityActions for the org.infinispan.scripting.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.2
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

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
      return doPrivileged(action);
   }

   static AuthorizationManager getAuthorizationManager(final AdvancedCache<?, ?> cache) {
      GetCacheAuthorizationManagerAction action = new GetCacheAuthorizationManagerAction(cache);
      return doPrivileged(action);
   }

   static <K, V> Configuration getCacheConfiguration(final Cache<K, V> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }

   static <K, V> CacheEntry<K, V> getCacheEntry(final AdvancedCache<K, V> cache, K key) {
      GetCacheEntryAction<K, V> action = new GetCacheEntryAction<K, V>(cache, key);
      return doPrivileged(action);
   }

   static <K, V> AdvancedCache<K, V> getUnwrappedCache(final Cache<K, V> cache) {
      UnwrapCacheAction<K, V> action = new UnwrapCacheAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }
}
