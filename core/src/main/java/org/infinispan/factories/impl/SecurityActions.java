package org.infinispan.factories.impl;

import java.lang.reflect.AccessibleObject;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * SecurityActions for the org.infinispan.factories package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
 * @since 9.4
 */
final class SecurityActions {
   private static final Log log = LogFactory.getLog(SecurityActions.class);

   static <T> void doPrivileged(PrivilegedAction<T> runnable) {
      if (System.getSecurityManager() != null) {
         AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            runnable.run();
            return null;
         });
      } else {
         Security.doPrivileged((PrivilegedAction<Void>) () -> {
            runnable.run();
            return null;
         });
      }
   }

   static void setAccessible(AccessibleObject member) {
      doPrivileged(() -> {
         member.setAccessible(true);
         return null;
      });
   }
}
