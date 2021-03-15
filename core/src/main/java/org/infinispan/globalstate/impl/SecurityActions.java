package org.infinispan.globalstate.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.DefineConfigurationAction;
import org.infinispan.security.actions.GetCacheAction;
import org.infinispan.security.actions.GetCacheConfigurationFromManagerAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.cli.interpreter.session package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void defineConfiguration(final EmbeddedCacheManager cacheManager, final String cacheName, final Configuration configurationOverride) {
      doPrivileged(new DefineConfigurationAction(cacheManager, cacheName, configurationOverride));
   }

   static void getCache(final EmbeddedCacheManager cacheManager, final String cacheName) {
      doPrivileged(new GetCacheAction(cacheManager, cacheName));
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }

   static Configuration getCacheConfiguration(EmbeddedCacheManager cacheManager, String name) {
      return doPrivileged(new GetCacheConfigurationFromManagerAction(cacheManager, name));
   }

   static void stopCache(Cache<?, ?> cache) {
      doPrivileged(() -> {cache.stop(); return null;});
   }
}
