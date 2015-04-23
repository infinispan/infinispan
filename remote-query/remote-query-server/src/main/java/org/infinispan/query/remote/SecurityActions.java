package org.infinispan.query.remote;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheConfigurationAction;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * SecurityActions for the org.infinispan.query.remote package. Do not move and do not change class and method
 * visibility!
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
final class SecurityActions {

   static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static Configuration getCacheConfiguration(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheConfigurationAction(cache));
   }
}
