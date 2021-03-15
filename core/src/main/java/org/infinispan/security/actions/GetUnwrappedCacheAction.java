package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * GetUnwrappedCacheAction.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class GetUnwrappedCacheAction<A extends Cache<K, V>, K, V> implements PrivilegedAction<A> {

   private final A cache;

   public GetUnwrappedCacheAction(A cache) {
      this.cache = cache;
   }

   @Override
   public A run() {
      if (cache instanceof SecureCacheImpl) {
         return (A) ((SecureCacheImpl) cache).getDelegate();
      } else {
         return (A) cache.getAdvancedCache();
      }
   }

}
