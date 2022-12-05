package org.infinispan.notifications.cachelistener.cluster;

import static org.infinispan.security.Security.doPrivileged;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.GetCacheAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.notifications.cachelistener.cluster package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.1
 */
final class SecurityActions {

   static <K, V> ComponentRegistry getComponentRegistry(AdvancedCache<K, V> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache));
   }

   static <K, V> Cache<K, V> getCache(EmbeddedCacheManager cacheManager, String cacheName) {
      return (Cache<K, V>) doPrivileged(new GetCacheAction(cacheManager, cacheName));
   }
}
