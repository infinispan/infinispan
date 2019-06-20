package org.infinispan.client.hotrod.near;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Near cache based on {@link BoundedConcurrentMapNearCache}
 *
 * @since 7.2
 */
final class BoundedConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, MetadataValue<V>> map;
   private final Cache<K, MetadataValue<V>> cache;

   private BoundedConcurrentMapNearCache(Cache<K, MetadataValue<V>> cache) {
      this.cache = cache;
      this.map = cache.asMap();
   }

   public static <K, V> NearCache<K, V> create(final NearCacheConfiguration config) {
      Cache<K, MetadataValue<V>> cache = Caffeine.newBuilder().maximumSize(config.maxEntries()).build();
      return new BoundedConcurrentMapNearCache<>(cache);
   }

   @Override
   public void put(K key, MetadataValue<V> value) {
      cache.put(key, value);
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      map.putIfAbsent(key, value);
   }

   @Override
   public boolean remove(K key) {
      return map.remove(key) != null;
   }

   @Override
   public MetadataValue<V> get(K key) {
      return map.get(key);
   }

   @Override
   public void clear() {
      map.clear();
   }

   @Override
   public int size() {
      // Make sure to clean up any evicted entries so the returned size is correct
      cache.cleanUp();
      return map.size();
   }
}
