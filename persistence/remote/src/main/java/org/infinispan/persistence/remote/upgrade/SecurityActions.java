package org.infinispan.persistence.remote.upgrade;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetClusterExecutorAction;

/**
 * @since 12.1
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static <K, V> ComponentRegistry getComponentRegistry(AdvancedCache<K, V> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache));
   }

   static ClusterExecutor getClusterExecutor(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetClusterExecutorAction(cacheManager));
   }
}
