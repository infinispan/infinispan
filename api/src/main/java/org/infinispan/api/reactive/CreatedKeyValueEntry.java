package org.infinispan.api.reactive;

public class CreatedKeyValueEntry<K, V> extends KeyValueEntry {
   public CreatedKeyValueEntry(Object key, Object value) {
      super(key, value);
   }
}
