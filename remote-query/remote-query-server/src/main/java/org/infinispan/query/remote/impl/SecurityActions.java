package org.infinispan.query.remote.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.query.remote package. Do not move and do not change class and method
 * visibility!
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
final class SecurityActions {

   private SecurityActions() {
   }

   static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache));
   }
}
