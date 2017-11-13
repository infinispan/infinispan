package org.infinispan.client.hotrod.counter.impl;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.concurrent.CompletableFuture;

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
                   NotificationManager notificationManager) {
      super(configuration, name, operationFactory, notificationManager);
      syncCounter = new Sync();
   }

   @Override
   public long getValue() {
      return await(factory.newGetValueOperation(name).execute());
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      return factory.newAddOperation(name, delta).execute().thenRun(() -> {});
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
         await(WeakCounterImpl.this.add(delta));
      }

      @Override
      public void reset() {
         await(WeakCounterImpl.this.reset());
      }

      @Override
      public CounterConfiguration getConfiguration() {
         return configuration;
      }

      @Override
      public void remove() {
         await(WeakCounterImpl.this.remove());
      }
   }
}
