package org.infinispan.client.hotrod.near;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.commons.util.CollectionFactory;

/**
 * Near cache based on {@link BoundedConcurrentMapNearCache}
 *
 * @since 7.2
 */
final class BoundedConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, MetadataValue<V>> cache;

   private BoundedConcurrentMapNearCache(ConcurrentMap<K, MetadataValue<V>> cache) {
      this.cache = cache;
   }

   public static <K, V> NearCache<K, V> create(final NearCacheConfiguration config) {
      return new BoundedConcurrentMapNearCache<K, V>(
         CollectionFactory.<K, MetadataValue<V>>makeBoundedConcurrentMap(config.maxEntries()));
   }

   @Override
   public void put(K key, MetadataValue<V> value) {
      cache.put(key, value);
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      cache.putIfAbsent(key, value);
   }

   @Override
   public boolean remove(K key) {
      return cache.remove(key) != null;
   }

   @Override
   public MetadataValue<V> get(K key) {
      return cache.get(key);
   }

   @Override
   public void clear() {
      cache.clear();
   }

   @Override
   public int size() {
      return cache.size();
   }
}
