package org.infinispan.counter.impl.listener;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.counter package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ryan Emerson
 * @since 10.1
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static ComponentRegistry getComponentRegistry(Cache cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache.getAdvancedCache()));
   }
}
