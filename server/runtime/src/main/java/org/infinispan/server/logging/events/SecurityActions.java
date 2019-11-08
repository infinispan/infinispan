package org.infinispan.server.logging.events;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetClusterExecutorAction;

/**
 * SecurityActions for the org.infinispan.stats.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ryan Emerson
 * @since 10.1
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static ClusterExecutor getClusterExecutor(final EmbeddedCacheManager cacheManager) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cacheManager);
      return doPrivileged(action);
   }
}
