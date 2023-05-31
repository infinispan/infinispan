package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetOrCreateCacheAction.
 *
 * @since 15.0
 */
public class GetOrCreateCacheAction<A extends Cache<K, V>, K, V> implements Supplier<A> {

   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;
   private final Configuration configuration;

   public GetOrCreateCacheAction(EmbeddedCacheManager cacheManager, String cacheName, Configuration configuration) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.configuration = configuration;
   }

   @SuppressWarnings("unchecked")
   @Override
   public A get() {
      return (A) cacheManager.administration().getOrCreateCache(cacheName, configuration);
   }

}
