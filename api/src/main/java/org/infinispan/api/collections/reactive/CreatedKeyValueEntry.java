package org.infinispan.api.collections.reactive;

public class CreatedKeyValueEntry<K, V> extends KeyValueEntry {
   public CreatedKeyValueEntry(Object key, Object value) {
      super(key, value);
   }
}
