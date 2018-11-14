package org.infinispan.rest.cachemanager;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listenable;
import org.infinispan.security.Security;
import org.infinispan.security.actions.AddCacheManagerListenerAction;
import org.infinispan.security.actions.RemoveListenerAction;

/**
 * SecurityActions for the org.infinispan.rest.cachemanager package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void addListener(EmbeddedCacheManager cacheManager, Object listener) {
      doPrivileged(new AddCacheManagerListenerAction(cacheManager, listener));
   }

   static Void removeListener(Listenable listenable, Object listener) {
      RemoveListenerAction action = new RemoveListenerAction(listenable, listener);
      return doPrivileged(action);
   }

}
