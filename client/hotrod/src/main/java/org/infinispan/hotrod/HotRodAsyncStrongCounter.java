package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.api.async.AsyncStrongCounter;
import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncStrongCounter implements AsyncStrongCounter {
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
      return null;
   }

   @Override
   public HotRodAsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<Long> value() {
      return null;
   }

   @Override
   public CompletionStage<Long> addAndGet(long delta) {
      return null;
   }

   @Override
   public CompletionStage<Void> reset() {
      return null;
   }

   @Override
   public CompletionStage<AutoCloseable> listen(Consumer<CounterEvent> listener) {
      return null;
   }

   @Override
   public CompletionStage<Long> compareAndSwap(long expect, long update) {
      return null;
   }

   @Override
   public CompletionStage<Long> getAndSet(long value) {
      return null;
   }
}
