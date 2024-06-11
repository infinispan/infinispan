package org.infinispan.client.hotrod.near;

import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

/**
 * @since 14.0
 **/
public class DefaultNearCacheFactory implements NearCacheFactory {
   public static final DefaultNearCacheFactory INSTANCE = new DefaultNearCacheFactory();

   @Override
   public <K, V> NearCache<K, V> createNearCache(NearCacheConfiguration config, BiConsumer<K, MetadataValue<V>> removedConsumer) {
      return config.maxEntries() > 0
            ? BoundedConcurrentMapNearCache.create(config, removedConsumer)
            : ConcurrentMapNearCache.create();
   }

   @Override
   public String toString() {
      return "DefaultNearCacheFactory{}";
   }
}
