package org.infinispan.notifications.cachelistener;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheInterceptorChainAction;
import org.infinispan.security.actions.GetDefaultExecutorServiceAction;

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

   static DefaultExecutorService getDefaultExecutorService(final Cache<?, ?> cache) {
      GetDefaultExecutorServiceAction action = new GetDefaultExecutorServiceAction(cache);
      return doPrivileged(action);
   }

   static List<CommandInterceptor> getInterceptorChain(final Cache<?, ?> cache) {
      GetCacheInterceptorChainAction action = new GetCacheInterceptorChainAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }
}
