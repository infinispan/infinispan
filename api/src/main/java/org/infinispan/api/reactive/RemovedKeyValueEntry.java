package org.infinispan.api.reactive;

public class RemovedKeyValueEntry<K, V> extends KeyValueEntry {
   public RemovedKeyValueEntry(K key, V value) {
      super(key, value);
   }

   @Override
   public EntryStatus status() {
      return EntryStatus.DELETED;
   }
}
