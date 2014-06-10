package org.infinispan.distexec;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheAuthorizationManagerAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;
import org.infinispan.security.actions.GetCacheDistributionManagerAction;
import org.infinispan.security.actions.GetCacheRpcManagerAction;

/**
 * SecurityActions for the org.infinispan.distexec package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
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

   static AuthorizationManager getCacheAuthorizationManager(final AdvancedCache<?, ?> cache) {
      GetCacheAuthorizationManagerAction action = new GetCacheAuthorizationManagerAction(cache);
      return doPrivileged(action);
   }

   static RpcManager getCacheRpcManager(final AdvancedCache<?, ?> cache) {
      GetCacheRpcManagerAction action = new GetCacheRpcManagerAction(cache);
      return doPrivileged(action);
   }

   static DistributionManager getCacheDistributionManager(final AdvancedCache<?, ?> cache) {
      GetCacheDistributionManagerAction action = new GetCacheDistributionManagerAction(cache);
      return doPrivileged(action);
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }
}
