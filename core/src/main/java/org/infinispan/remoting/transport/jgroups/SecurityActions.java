package org.infinispan.remoting.transport.jgroups;

import org.infinispan.security.Security;
import org.infinispan.security.actions.GetSystemPropertyAsBooleanAction;
import org.infinispan.security.actions.GetSystemPropertyAsIntegerAction;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Privileged actions for the package
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
 * @since 8.2
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }


   static boolean getBooleanProperty(String name) {
      return doPrivileged(new GetSystemPropertyAsBooleanAction(name));
   }

   static int getIntProperty(String name, int defaultValue) {
      return doPrivileged(new GetSystemPropertyAsIntegerAction(name, defaultValue));
   }
}
