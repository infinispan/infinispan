package org.infinispan.query.dsl.embedded.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.query.dsl.embedded.impl package. Do not move and do not change class and
 * method visibility!
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static ComponentRegistry getCacheComponentRegistry(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getComponentRegistry);
   }

   static SearchManager getCacheSearchManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(() -> Search.getSearchManager(cache));
   }
}
