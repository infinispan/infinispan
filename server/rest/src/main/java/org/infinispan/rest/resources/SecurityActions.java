package org.infinispan.rest.resources;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.Health;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetCacheConfigurationFromManagerAction;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetCacheManagerHealthAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.impl.Authorizer;

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

   static Configuration getCacheConfiguration(AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }

   static Configuration getCacheConfigurationFromManager(final EmbeddedCacheManager cacheManager, String cacheName) {
      GetCacheConfigurationFromManagerAction action = new GetCacheConfigurationFromManagerAction(cacheManager,
            cacheName);
      return doPrivileged(action);
   }

   static Health getHealth(final EmbeddedCacheManager cacheManager) {
      GetCacheManagerHealthAction action = new GetCacheManagerHealthAction(cacheManager);
      return doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(cacheManager));
   }

   public static EncoderRegistry getEncoderRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(() -> cache.getCacheManager().getGlobalComponentRegistry().getComponent(EncoderRegistry.class));
   }

   static <K, V> ComponentRegistry getComponentRegistry(AdvancedCache<K, V> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache));
   }

   static void checkPermission(InvocationHelper invocationHelper, RestRequest request, AuthorizationPermission permission) {
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      Authorizer authorizer = getGlobalComponentRegistry(cacheManager).getComponent(Authorizer.class);
      authorizer.checkPermission(request.getSubject(), permission);
   }
}
