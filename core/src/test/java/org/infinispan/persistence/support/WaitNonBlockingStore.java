package org.infinispan.persistence.support;

import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.CompletionStages;

public interface WaitNonBlockingStore<K, V> extends NonBlockingStore<K, V> {
   KeyPartitioner getKeyPartitioner();

   default boolean delete(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return CompletionStages.join(delete(segment, key));
   }

   default boolean contains(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return CompletionStages.join(containsKey(segment, key));
   }

   default MarshallableEntry<K, V> loadEntry(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return CompletionStages.join(load(segment, key));
   }

   default void write(MarshallableEntry<K, V> entry) {
      int segment = getKeyPartitioner().getSegment(entry.getKey());
      CompletionStages.join(write(segment, entry));
   }

   default boolean checkAvailable() {
      return CompletionStages.join(isAvailable());
   }
}
