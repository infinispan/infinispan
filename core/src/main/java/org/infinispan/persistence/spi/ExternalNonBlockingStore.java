package org.infinispan.persistence.spi;

import java.util.concurrent.CompletionStage;

public interface ExternalNonBlockingStore<K, V> extends NonBlockingStore<K, V> {
   default CompletionStage<Void> destroy() {
      return stop();
   }
}
