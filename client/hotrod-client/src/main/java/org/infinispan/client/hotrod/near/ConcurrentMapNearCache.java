package org.infinispan.client.hotrod.near;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.commons.util.CollectionFactory;

/**
 * A concurrent-map-based near cache implementation.
 * It does not provide eviction capabilities.
 *
 * @since 7.1
 */
final class ConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, MetadataValue<V>> cache = CollectionFactory.makeConcurrentMap();

   @Override
   public void put(K key, MetadataValue<V> value) {
      cache.put(key, value);
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      cache.putIfAbsent(key, value);
   }

   @Override
   public void remove(K key) {
      cache.remove(key);
   }

   @Override
   public MetadataValue<V> get(Object key) {
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
