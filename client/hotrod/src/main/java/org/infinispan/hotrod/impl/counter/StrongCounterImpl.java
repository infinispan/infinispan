package org.infinispan.hotrod.impl.counter;

import static org.infinispan.hotrod.impl.Util.await;

import java.util.concurrent.CompletableFuture;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;

/**
 * A {@link StrongCounter} implementation for Hot Rod clients.
 *
 * @since 14.0
 */
class StrongCounterImpl extends BaseCounter implements StrongCounter {
   private final SyncStrongCounter syncCounter;

   StrongCounterImpl(String name, CounterConfiguration configuration, CounterOperationFactory operationFactory,
                     NotificationManager notificationManager) {
      super(configuration, name, operationFactory, notificationManager);
      this.syncCounter = new Sync();
   }

   public CompletableFuture<Long> getValue() {
      return factory.newGetValueOperation(name, useConsistentHash()).execute().toCompletableFuture();
   }

   public CompletableFuture<Long> addAndGet(long delta) {
      return factory.newAddOperation(name, delta, useConsistentHash()).execute().toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return factory.newCompareAndSwapOperation(name, expect, update, super.getConfiguration()).execute().toCompletableFuture();
   }

   @Override
   public SyncStrongCounter sync() {
      return syncCounter;
   }

   @Override
   public CompletableFuture<Long> getAndSet(long value) {
      return factory.newSetOperation(name, value, useConsistentHash()).execute().toCompletableFuture();
   }

   @Override
   boolean useConsistentHash() {
      return true;
   }

   private class Sync implements SyncStrongCounter {

      @Override
      public long addAndGet(long delta) {
         return await(StrongCounterImpl.this.addAndGet(delta));
      }

      @Override
      public void reset() {
         await(StrongCounterImpl.this.reset());
      }

      @Override
      public long getValue() {
         return await(StrongCounterImpl.this.getValue());
      }

      @Override
      public long compareAndSwap(long expect, long update) {
         return await(StrongCounterImpl.this.compareAndSwap(expect, update));
      }

      @Override
      public long getAndSet(long value) {
         return await(StrongCounterImpl.this.getAndSet(value));
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public CounterConfiguration getConfiguration() {
         return configuration;
      }

      @Override
      public void remove() {
         await(StrongCounterImpl.this.remove());
      }
   }
}
