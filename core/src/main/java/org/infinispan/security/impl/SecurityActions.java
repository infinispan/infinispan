package org.infinispan.security.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.AddCacheDependencyAction;

/**
 * SecurityActions for the org.infinispan.security.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Dan Berindei
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

   static void addCacheDependency(EmbeddedCacheManager cacheManager, String from, String to) {
      doPrivileged(new AddCacheDependencyAction(cacheManager, from, to));
   }
}
