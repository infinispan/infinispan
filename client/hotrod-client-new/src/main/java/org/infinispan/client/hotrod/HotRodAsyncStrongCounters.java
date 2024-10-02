package org.infinispan.client.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncStrongCounter;
import org.infinispan.api.async.AsyncStrongCounters;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncStrongCounters implements AsyncStrongCounters {
   private final HotRod hotrod;

   HotRodAsyncStrongCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public CompletionStage<AsyncStrongCounter> get(String name) {
      return CompletableFuture.completedFuture(new HotRodAsyncStrongCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<AsyncStrongCounter> create(String name, CounterConfiguration configuration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<String> names() {
      throw new UnsupportedOperationException();
   }
}
