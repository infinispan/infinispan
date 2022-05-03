package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncWeakCounter;
import org.infinispan.api.async.AsyncWeakCounters;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncWeakCounters implements AsyncWeakCounters {
   private final HotRod hotrod;

   HotRodAsyncWeakCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public CompletionStage<AsyncWeakCounter> get(String name) {
      return CompletableFuture.completedFuture(new HotRodAsyncWeakCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<AsyncWeakCounter> create(String name, CounterConfiguration configuration) {
      return CompletableFuture.completedFuture(new HotRodAsyncWeakCounter(hotrod, name)); // PLACEHOLDER
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
