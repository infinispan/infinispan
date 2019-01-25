package org.infinispan.manager.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.security.Security;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * SecurityActions for the org.infinispan.manager.impl package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @since 10.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static <K, V> Cache<K, V> getUnwrappedCache(Cache<K, V> cache) {
      if (cache instanceof SecureCacheImpl) {
         return doPrivileged(((SecureCacheImpl<K, V>) cache)::getDelegate);
      } else {
         return cache;
      }
   }
}
