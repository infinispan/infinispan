package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.commons.util.CollectionFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * Near cache based on {@link BoundedConcurrentMapNearCache}
 *
 * @since 7.2
 */
final class BoundedConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, VersionedValue<V>> cache;

   private BoundedConcurrentMapNearCache(ConcurrentMap<K, VersionedValue<V>> cache) {
      this.cache = cache;
   }

   public static <K, V> NearCache<K, V> create(final NearCacheConfiguration config) {
      return new BoundedConcurrentMapNearCache<K, V>(
         CollectionFactory.<K, VersionedValue<V>>makeBoundedConcurrentMap(config.maxEntries()));
   }

   @Override
   public void put(K key, VersionedValue<V> value) {
      cache.put(key, value);
   }

   @Override
   public void putIfAbsent(K key, VersionedValue<V> value) {
      cache.putIfAbsent(key, value);
   }

   @Override
   public void remove(K key) {
      cache.remove(key);
   }

   @Override
   public VersionedValue<V> get(K key) {
      return cache.get(key);
   }

   @Override
   public void clear() {
      cache.clear();
   }

}
