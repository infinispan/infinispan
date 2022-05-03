package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncStrongCounter;
import org.infinispan.api.async.AsyncStrongCounters;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncStrongCounters implements AsyncStrongCounters {
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
      return CompletableFuture.completedFuture(new HotRodAsyncStrongCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return null;
   }

   @Override
   public Flow.Publisher<String> names() {
      return null;
   }
}
