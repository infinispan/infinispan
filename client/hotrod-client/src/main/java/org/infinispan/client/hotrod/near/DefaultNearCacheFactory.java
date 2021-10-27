package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

/**
 * @since 14.0
 **/
public class DefaultNearCacheFactory implements NearCacheFactory {
   public static final DefaultNearCacheFactory INSTANCE = new DefaultNearCacheFactory();

   @Override
   public <K, V> NearCache<K, V> createNearCache(NearCacheConfiguration config) {
      return config.maxEntries() > 0
            ? BoundedConcurrentMapNearCache.create(config)
            : ConcurrentMapNearCache.create();
   }

   @Override
   public String toString() {
      return "DefaultNearCacheFactory{}";
   }
}
