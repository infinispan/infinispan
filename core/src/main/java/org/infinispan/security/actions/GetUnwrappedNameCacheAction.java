package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * GetUnwrappedNameCacheAction.
 *
 * @since 15.0
 */
public class GetUnwrappedNameCacheAction<A extends Cache<K, V>, K, V> implements Supplier<A> {
   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;

   public GetUnwrappedNameCacheAction(EmbeddedCacheManager cacheManager, String cacheName) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
   }

   @Override
   public A get() {
      Cache<?, ?> cache = cacheManager.getCache(cacheName);
      if (cache instanceof SecureCacheImpl) {
         return (A) ((SecureCacheImpl) cache).getDelegate();
      } else {
         return (A) cache.getAdvancedCache();
      }
   }

}
