package org.infinispan.distexec;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;

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
   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<ComponentRegistry>() {
            @Override
            public ComponentRegistry run() {
               return cache.getComponentRegistry();
            }
         });
      } else {
         return cache.getComponentRegistry();
      }
   }

   static AuthorizationManager getCacheAuthorizationManager(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<AuthorizationManager>() {
            @Override
            public AuthorizationManager run() {
               return cache.getAuthorizationManager();
            }
         });
      } else {
         return cache.getAuthorizationManager();
      }
   }

   static RpcManager getCacheRpcManager(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<RpcManager>() {
            @Override
            public RpcManager run() {
               return cache.getRpcManager();
            }
         });
      } else {
         return cache.getRpcManager();
      }
   }

   static DistributionManager getCacheDistributionManager(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<DistributionManager>() {
            @Override
            public DistributionManager run() {
               return cache.getDistributionManager();
            }
         });
      } else {
         return cache.getDistributionManager();
      }
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<Configuration>() {
            @Override
            public Configuration run() {
               return cache.getCacheConfiguration();
            }
         });
      } else {
         return cache.getCacheConfiguration();
      }
   }
}
