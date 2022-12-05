package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * GetUnwrappedCacheAction.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class GetUnwrappedCacheAction<A extends Cache<K, V>, K, V> implements Supplier<A> {

   private final Cache<K, V> cache;

   public GetUnwrappedCacheAction(Cache<K, V> cache) {
      this.cache = cache;
   }

   @Override
   public A get() {
      if (cache instanceof SecureCacheImpl) {
         return (A) ((SecureCacheImpl) cache).getDelegate();
      } else {
         return (A) cache.getAdvancedCache();
      }
   }

}
