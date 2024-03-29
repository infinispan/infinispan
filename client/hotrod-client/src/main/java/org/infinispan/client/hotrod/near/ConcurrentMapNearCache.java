package org.infinispan.client.hotrod.near;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * A concurrent-map-based near cache implementation.
 * It does not provide eviction capabilities.
 *
 * @since 7.1
 */
final class ConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, MetadataValue<V>> cache = new ConcurrentHashMap<>();

   @Override
   public boolean putIfAbsent(K key, MetadataValue<V> value) {
      return cache.putIfAbsent(key, value) == null;
   }

   @Override
   public boolean replace(K key, MetadataValue<V> prevValue, MetadataValue<V> newValue) {
      return cache.replace(key, prevValue, newValue);
   }

   @Override
   public boolean remove(K key) {
      return cache.remove(key) != null;
   }

   @Override
   public boolean remove(K key, MetadataValue<V> value) {
      return cache.remove(key, value);
   }

   @Override
   public MetadataValue<V> get(Object key) {
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

   public static <K, V> NearCache<K, V> create() {
      return new ConcurrentMapNearCache<K, V>();
   }


   @Override
   public Iterator<Map.Entry<K, MetadataValue<V>>> iterator() {
      return cache.entrySet().iterator();
   }
}
