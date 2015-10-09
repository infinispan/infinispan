package org.infinispan.configuration.global;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.security.actions.GetSystemPropertyAction;

/**
 * SecurityActions for the org.infinispan.configuration.global package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static String getSystemProperty(String propertyName) {
      GetSystemPropertyAction action = new GetSystemPropertyAction(propertyName);
      return doPrivileged(action);
   }


}
