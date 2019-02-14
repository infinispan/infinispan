package org.infinispan.server.core.utils;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.security.actions.GetSystemPropertyAction;

/**
 * SecurityActions for the org.infinispan.server.core.utils package. Do not move and do not change class and method
 * visibility!
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static String getSystemProperty(String propertyName) {
      return doPrivileged(new GetSystemPropertyAction(propertyName));
   }
}
