package org.infinispan.server.hotrod;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listenable;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetCacheGlobalComponentRegistryAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.actions.RemoveListenerAction;

/**
 * SecurityActions for the org.infinispan.server.hotrod package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
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

   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      return doPrivileged(action);
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }

   @SuppressWarnings("unchecked")
   static <K, V> org.infinispan.Cache<K, V> getCache(final EmbeddedCacheManager cacheManager, String cacheName) {
      GetCacheAction action = new GetCacheAction(cacheManager, cacheName);
      return (org.infinispan.Cache<K, V>) doPrivileged(action);
   }

   static GlobalComponentRegistry getCacheGlobalComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheGlobalComponentRegistryAction action = new GetCacheGlobalComponentRegistryAction(cache);
      return doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
      return doPrivileged(action);
   }

   static Void removeListener(Listenable listenable, Object listener) {
      RemoveListenerAction action = new RemoveListenerAction(listenable, listener);
      return doPrivileged(action);
   }
}
