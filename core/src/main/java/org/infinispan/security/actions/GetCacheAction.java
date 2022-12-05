package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheAction<A extends Cache<K, V>, K, V> implements Supplier<A> {

   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;

   public GetCacheAction(EmbeddedCacheManager cacheManager, String cacheName) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
   }

   @Override
   public A get() {
      return (A)cacheManager.getCache(cacheName);
   }

}
