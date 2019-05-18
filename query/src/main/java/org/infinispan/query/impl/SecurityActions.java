package org.infinispan.query.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.AddCacheDependencyAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.query.impl package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static String getSystemProperty(String propertyName) {
      return doPrivileged(() -> System.getProperty(propertyName));
   }

   static Configuration getCacheConfiguration(Cache<?, ?> cache) {
      return doPrivileged(cache::getCacheConfiguration);
   }

   static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache));
   }

   static void addCacheDependency(EmbeddedCacheManager cacheManager, String cacheStarting, String queryKnownClassesCacheName) {
      doPrivileged(new AddCacheDependencyAction(cacheManager, cacheStarting, queryKnownClassesCacheName));
   }
}
