package org.infinispan.notifications.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.security.Security;
import org.infinispan.security.actions.SetThreadContextClassLoaderAction;

/**
 * SecurityActions for the org.infinispan.notifications.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static ClassLoader setContextClassLoader(ClassLoader contextClassLoader) {
      SetThreadContextClassLoaderAction action = new SetThreadContextClassLoaderAction(contextClassLoader);
      return doPrivileged(action);
   }
}
