package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * Near cache contract.
 *
 * @since 7.1
 */
interface NearCache<K, V> {
   void put(K key, MetadataValue<V> value);
   void putIfAbsent(K key, MetadataValue<V> value);
   void remove(K key);
   MetadataValue<V> get(K key);
   void clear();
}
