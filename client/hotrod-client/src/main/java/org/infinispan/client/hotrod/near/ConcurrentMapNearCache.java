package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.util.CollectionFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent-map-based near cache implementation.
 * It does not provide eviction capabilities.
 *
 * @since 7.1
 */
final class ConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, VersionedValue<V>> cache = CollectionFactory.makeConcurrentMap();

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
   public VersionedValue<V> get(Object key) {
      return cache.get(key);
   }

   @Override
   public void clear() {
      cache.clear();
   }

   public static <K, V> NearCache<K, V> create() {
      return new ConcurrentMapNearCache<K, V>();
   }

}
