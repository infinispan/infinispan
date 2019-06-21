package org.infinispan.api.reactive;

public class DeletedKeyValueEntry<K, V> extends KeyValueEntry {
   public DeletedKeyValueEntry(K key, V value) {
      super(key, value);
   }

   @Override
   public EntryStatus status() {
      return EntryStatus.DELETED;
   }
}
