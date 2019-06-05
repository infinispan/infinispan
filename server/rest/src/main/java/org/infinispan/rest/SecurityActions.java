package org.infinispan.rest;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.rest package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 */
final class SecurityActions {

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ? AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }
}
