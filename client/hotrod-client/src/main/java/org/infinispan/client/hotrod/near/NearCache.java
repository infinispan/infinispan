package org.infinispan.client.hotrod.near;

import java.util.Map;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * Near cache contract.
 *
 * @since 7.1
 */
public interface NearCache<K, V> extends Iterable<Map.Entry<K, MetadataValue<V>>> {
   boolean putIfAbsent(K key, MetadataValue<V> value);
   boolean replace(K key, MetadataValue<V> prevValue, MetadataValue<V> newValue);
   boolean remove(K key);

   // Removes a specific value from the near cache, note this method does not count towards and invalidation
   boolean remove(K key, MetadataValue<V> value);
   MetadataValue<V> get(K key);
   void clear();
   int size();
}
