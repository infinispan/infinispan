package org.infinispan.client.hotrod.near;

import java.util.Map;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * Near cache contract.
 *
 * @since 7.1
 */
public interface NearCache<K, V> extends Iterable<Map.Entry<K, MetadataValue<V>>> {
   void put(K key, MetadataValue<V> value);
   void putIfAbsent(K key, MetadataValue<V> value);
   boolean remove(K key);
   MetadataValue<V> get(K key);
   void clear();
   int size();
}
