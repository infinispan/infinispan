package org.infinispan.notifications.cachelistener;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheInterceptorChainAction;
import org.infinispan.security.actions.GetClusterExecutorAction;

/**
 * SecurityActions for the org.infinispan.notifications.cachelistener package.
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

   static ClusterExecutor getClusterExecutor(final Cache<?, ?> cache) {
      GetClusterExecutorAction action = new GetClusterExecutorAction(cache);
      return doPrivileged(action);
   }

   static List<AsyncInterceptor> getInterceptorChain(final Cache<?, ?> cache) {
      GetCacheInterceptorChainAction action = new GetCacheInterceptorChainAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }

   static AsyncInterceptorChain getAsyncInterceptorChain(final Cache<?, ?> cache) {
      return doPrivileged(() -> cache.getAdvancedCache().getAsyncInterceptorChain());
   }
}
