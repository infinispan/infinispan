package org.infinispan.persistence.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.persistence.spi.MarshallableEntry;

class ClearModification implements Modification {
   private ClearModification() {
   }

   public static final ClearModification INSTANCE = new ClearModification();

   @Override
   public <K, V> void apply(AsyncNonBlockingStore<K, V> store) {
      store.putClearModification();
   }

   @Override
   public int getSegment() {
      throw new UnsupportedOperationException("This should never be invoked");
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> asStage() {
      throw new UnsupportedOperationException("This should never be invoked");
   }

   @Override
   public String toString() {
      return "ClearModification{}";
   }
}
