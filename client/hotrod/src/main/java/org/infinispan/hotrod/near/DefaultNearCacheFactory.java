package org.infinispan.hotrod.near;

import java.util.function.Consumer;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.hotrod.configuration.NearCache;
import org.infinispan.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.hotrod.configuration.NearCacheFactory;

/**
 * @since 14.0
 **/
public class DefaultNearCacheFactory implements NearCacheFactory {
   public static final DefaultNearCacheFactory INSTANCE = new DefaultNearCacheFactory();

   @Override
   public <K, V> NearCache<K, V> createNearCache(NearCacheConfiguration config, Consumer<CacheEntry<K, V>> removedConsumer) {
      return config.maxEntries() > 0
            ? BoundedConcurrentMapNearCache.create(config, removedConsumer)
            : ConcurrentMapNearCache.create();
   }

   @Override
   public String toString() {
      return "DefaultNearCacheFactory{}";
   }
}
