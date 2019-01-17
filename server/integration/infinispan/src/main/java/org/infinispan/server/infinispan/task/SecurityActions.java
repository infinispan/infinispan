package org.infinispan.server.infinispan.task;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.server.infinispan.task package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @since 10.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static ClusterExecutor getClusterExecutor(EmbeddedCacheManager embeddedCacheManager) {
      return doPrivileged(embeddedCacheManager::executor);
   }
}
