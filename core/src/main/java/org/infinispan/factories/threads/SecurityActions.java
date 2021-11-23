package org.infinispan.factories.threads;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.factories.threads package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
 * @since 14.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void setContextClassLoader(Thread thread, ClassLoader contextClassLoader) {
      doPrivileged(() -> {
         thread.setContextClassLoader(contextClassLoader);
         return null;
      });
   }
}
