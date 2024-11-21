package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncStrongCounter;
import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncStrongCounter implements AsyncStrongCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodAsyncStrongCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<CounterConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public AsyncContainer container() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> value() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> addAndGet(long delta) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> reset() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<AutoCloseable> listen(Consumer<CounterEvent> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> compareAndSwap(long expect, long update) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> getAndSet(long value) {
      throw new UnsupportedOperationException();
   }
}
