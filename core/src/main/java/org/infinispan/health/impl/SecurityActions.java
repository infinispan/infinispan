package org.infinispan.health.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAction;
import org.infinispan.security.actions.GetCacheDistributionManagerAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

final class SecurityActions {

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static DistributionManager getDistributionManager(final Cache<?, ?> cache) {
      GetCacheDistributionManagerAction action = new GetCacheDistributionManagerAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }

   static Cache<?, ?> getCache(final EmbeddedCacheManager cacheManager, final String cacheName) {
      return doPrivileged(new GetCacheAction(cacheManager, cacheName));
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }
}
