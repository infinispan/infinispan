package org.infinispan.api.reactive;

public class UpdatedKeyValueEntry<K, V> extends KeyValueEntry {
   public UpdatedKeyValueEntry(K key, V value) {
      super(key, value);
   }

   @Override
   public EntryStatus status() {
      return EntryStatus.UPDATED;
   }
}
