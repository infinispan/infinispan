package org.infinispan.persistence.jdbc.common.connectionfactory;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.security.actions.GetClassInstanceAction;

/**
 * SecurityActions for the org.infinispan.persistence.jdbc.connectionfactory package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static  <T> T getClassInstance(Class<T> clazz) {
      GetClassInstanceAction<T> action = new GetClassInstanceAction<T>(clazz);
      return doPrivileged(action);
   }
}
