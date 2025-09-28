package org.infinispan.client.hotrod.counter.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A {@link WeakCounter} implementation for Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class WeakCounterImpl extends BaseCounter implements WeakCounter {
   private final SyncWeakCounter syncCounter;

   WeakCounterImpl(String name, CounterConfiguration configuration, CounterOperationFactory operationFactory,
                   OperationDispatcher dispatcher, NotificationManager notificationManager) {
      super(configuration, name, operationFactory, dispatcher, notificationManager);
      syncCounter = new Sync();
   }

   @Override
   public long getValue() {
      return dispatcher.await(dispatcher.execute(factory.newGetValueOperation(name, useConsistentHash())));
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      return dispatcher.execute(factory.newAddOperation(name, delta, useConsistentHash()))
            .toCompletableFuture().thenRun(() -> {});
   }

   @Override
   public SyncWeakCounter sync() {
      return syncCounter;
   }

   private class Sync implements SyncWeakCounter {

      @Override
      public String getName() {
         return name;
      }

      @Override
      public long getValue() {
         return WeakCounterImpl.this.getValue();
      }

      @Override
      public void add(long delta) {
         dispatcher.await(WeakCounterImpl.this.add(delta));
      }

      @Override
      public void reset() {
         dispatcher.await(WeakCounterImpl.this.reset());
      }

      @Override
      public CounterConfiguration getConfiguration() {
         return configuration;
      }

      @Override
      public void remove() {
         dispatcher.await(WeakCounterImpl.this.remove());
      }
   }
}
