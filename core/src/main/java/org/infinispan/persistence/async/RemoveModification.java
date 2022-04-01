package org.infinispan.persistence.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.commons.util.concurrent.CompletableFutures;

class RemoveModification implements Modification {
   private final int segment;
   private final Object key;

   RemoveModification(int segment, Object key) {
      this.segment = segment;
      this.key = key;
   }

   @Override
   public <K, V> void apply(AsyncNonBlockingStore<K, V> store) {
      store.putModification(AsyncNonBlockingStore.wrapKeyIfNeeded(key), this);
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> asStage() {
      return CompletableFutures.completedNull();
   }

   public Object getKey() {
      return key;
   }

   @Override
   public String toString() {
      return "RemoveModification{" +
            "segment=" + segment +
            ", key=" + key +
            '}';
   }
}
