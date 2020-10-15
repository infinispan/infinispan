package org.infinispan.persistence.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.persistence.spi.MarshallableEntry;

class PutModification implements Modification {
   private final int segment;
   private final MarshallableEntry entry;

   PutModification(int segment, MarshallableEntry entry) {
      this.segment = segment;
      this.entry = entry;
   }

   @Override
   public <K, V> void apply(AsyncNonBlockingStore<K, V> store) {
      store.putModification(AsyncNonBlockingStore.wrapKeyIfNeeded(entry.getKey()), this);
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @SuppressWarnings("unchecked")
   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> asStage() {
      return CompletableFuture.completedFuture(entry);
   }

   @SuppressWarnings("unchecked")
   public <K, V> MarshallableEntry<K, V> getEntry() {
      return entry;
   }

   @Override
   public String toString() {
      return "PutModification{" +
            "segment=" + segment +
            ", entry=" + entry +
            '}';
   }
}
