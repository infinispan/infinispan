package org.infinispan.server.core.backup;

/**
 * SecurityActions for the org.infinispan.server.server package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ryan Emerson
 * @since 12.0
 */

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;

final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static GlobalConfiguration getGlobalConfiguration(final EmbeddedCacheManager cacheManager) {
      GetCacheManagerConfigurationAction action = new GetCacheManagerConfigurationAction(cacheManager);
      return doPrivileged(action);
   }
}
