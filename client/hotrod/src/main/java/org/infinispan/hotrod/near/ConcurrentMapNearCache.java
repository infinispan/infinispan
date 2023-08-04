package org.infinispan.hotrod.near;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.hotrod.configuration.NearCache;

/**
 * A concurrent-map-based near cache implementation.
 * It does not provide eviction capabilities.
 *
 * @since 14.0
 */
final class ConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, CacheEntry<K, V>> cache = new ConcurrentHashMap<>();

   @Override
   public boolean putIfAbsent(K key, CacheEntry<K, V> entry) {
      return cache.putIfAbsent(key, entry) == null;
   }

   @Override
   public boolean replace(K key, CacheEntry<K, V> prevValue, CacheEntry<K, V> newValue) {
      return cache.replace(key, prevValue, newValue);
   }

   @Override
   public boolean remove(K key) {
      return cache.remove(key) != null;
   }

   @Override
   public boolean remove(K key, CacheEntry<K, V> entry) {
      return cache.remove(key, entry);
   }

   @Override
   public CacheEntry<K, V> get(K key) {
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
   public Iterator<CacheEntry<K, V>> iterator() {
      return cache.values().stream().iterator();
   }
}
