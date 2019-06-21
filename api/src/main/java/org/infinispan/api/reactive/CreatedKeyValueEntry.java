package org.infinispan.api.reactive;

public class CreatedKeyValueEntry<K, V> extends KeyValueEntry {
   public CreatedKeyValueEntry(K key, V value) {
      super(key, value);
   }
}
