package org.infinispan.rest.operations;

import java.security.AccessController;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.security.Security;
import org.infinispan.security.actions.ContextAwarePrivilegedAction;
import org.infinispan.security.actions.GetCacheConfigurationAction;

/**
 * SecurityActions for the org.infinispan.rest.operations package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
final class SecurityActions {
   private static <T> T doPrivileged(ContextAwarePrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else if (action.contextRequiresSecurity()) {
         return Security.doPrivileged(action);
      } else {
         return action.run();
      }
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }
}
