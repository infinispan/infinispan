package org.infinispan.configuration.parsing;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;

import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.configuration.parsing
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
final class SecurityActions {

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static Properties getSystemProperties() {
      return doPrivileged(System::getProperties);
   }
}
