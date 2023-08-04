package org.infinispan.hotrod.configuration;

import org.infinispan.api.common.CacheEntry;

/**
 * Near cache contract.
 *
 * @since 14.0
 */
public interface NearCache<K, V> extends Iterable<CacheEntry<K, V>> {
   boolean putIfAbsent(K key, CacheEntry<K, V> entry);
   boolean replace(K key, CacheEntry<K, V> prevValue, CacheEntry<K, V> newValue);
   boolean remove(K key);

   // Removes a specific value from the near cache, note this method does not count towards and invalidation
   boolean remove(K key, CacheEntry<K, V> entry);
   CacheEntry<K, V> get(K key);
   void clear();
   int size();
}
