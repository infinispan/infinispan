package org.infinispan.remoting.transport.jgroups;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.security.actions.GetSystemPropertyAction;

/**
 * SecurityActions for the org.infinispan.remoting.transport.jgroups package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @since 15.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static String getSystemProperty(String propertyName, String defaultValue) {
      GetSystemPropertyAction action = new GetSystemPropertyAction(propertyName, defaultValue);
      return doPrivileged(action);
   }


}
