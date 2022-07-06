package org.infinispan.hotrod.util;

import java.util.Collections;
import java.util.Map;

import org.infinispan.hotrod.test.KeyValueGenerator;

/**
 * Small helper that wraps a view of the entries in the cache with the {@link KeyValueGenerator}.
 * This is necessary to correctly find the keys within the map for different types of keys.
 * <p/>
 * Most operations need to iterate over the entry set and check using the {@link KeyValueGenerator}
 * to check for equality.
 *
 * @param <K>: The type of keys.
 * @param <V>: The type of values.
 */
public class MapKVHelper<K, V> {
   private final Map<K, V> entries;
   private final KeyValueGenerator<K, V> kvGenerator;

   public MapKVHelper(Map<K, V> entries, KeyValueGenerator<K, V> kvGenerator) {
      this.entries = Collections.unmodifiableMap(entries);
      this.kvGenerator = kvGenerator;
   }

   public V get(K key) {
      for (Map.Entry<K, V> entry: entries.entrySet()) {
         if (kvGenerator.equalKeys(entry.getKey(), key)) {
            return entry.getValue();
         }
      }
      return null;
   }

   public boolean contains(K key) {
      return get(key) != null;
   }
}
