package org.infinispan.hotrod.configuration;

import org.infinispan.api.common.CacheEntry;

/**
 * Near cache contract.
 *
 * @since 14.0
 */
public interface NearCache<K, V> extends Iterable<CacheEntry<K, V>> {
   void put(CacheEntry<K, V>  entry);
   void putIfAbsent(CacheEntry<K, V> entry);
   boolean remove(K key);
   CacheEntry<K, V> get(K key);
   void clear();
   int size();
}
