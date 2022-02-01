package org.infinispan.test;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.DefineConfigurationAction;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.test.fwk package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
 * @since 10.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void stopManager(EmbeddedCacheManager cacheManager) {
      doPrivileged(() -> {
         cacheManager.stop();
         return null;
      });
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }

   static ComponentRegistry getComponentRegistry(Cache cache) {
      return doPrivileged(() -> cache.getAdvancedCache().getComponentRegistry());
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(cacheManager));
   }

   static void defineConfiguration(final EmbeddedCacheManager cacheManager, final String cacheName, final Configuration configurationOverride) {
      doPrivileged(new DefineConfigurationAction(cacheManager, cacheName, configurationOverride));
   }
}
