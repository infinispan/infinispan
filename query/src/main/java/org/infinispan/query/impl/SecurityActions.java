package org.infinispan.query.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;

/**
 * SecurityActions for the org.infinispan.query.impl package.
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
