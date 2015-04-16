package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.VersionedValue;

/**
 * Near cache contract.
 *
 * @since 7.1
 */
interface NearCache<K, V> {
   void put(K key, VersionedValue<V> value);
   void putIfAbsent(K key, VersionedValue<V> value);
   void remove(K key);
   VersionedValue<V> get(K key);
   void clear();
}
