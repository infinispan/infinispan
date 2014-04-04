package org.infinispan.notifications.cachelistener;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.util.concurrent.WithinThreadExecutor;

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
   static DefaultExecutorService getDefaultExecutorService(final Cache<?, ?> cache) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<DefaultExecutorService>() {
            @Override
            public DefaultExecutorService run() {
               return new DefaultExecutorService(cache, new WithinThreadExecutor());
            }
         });
      } else {
         return new DefaultExecutorService(cache, new WithinThreadExecutor());
      }
   }
}
