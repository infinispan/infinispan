package org.infinispan.client.hotrod.counter.impl;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;

/**
 * A {@link StrongCounter} implementation for Hot Rod clients.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class StrongCounterImpl extends BaseCounter implements StrongCounter {
   private final SyncStrongCounter syncCounter;

   StrongCounterImpl(String name, CounterConfiguration configuration, CounterOperationFactory operationFactory,
                     OperationDispatcher dispatcher, NotificationManager notificationManager) {
      super(configuration, name, operationFactory, dispatcher, notificationManager);
      this.syncCounter = new Sync();
   }

   public CompletableFuture<Long> getValue() {
      return dispatcher.execute(factory.newGetValueOperation(name, useConsistentHash()))
            .toCompletableFuture();
   }

   public CompletableFuture<Long> addAndGet(long delta) {
      return dispatcher.execute(factory.newAddOperation(name, delta, useConsistentHash()))
            .toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return dispatcher.execute(factory.newCompareAndSwapOperation(name, expect, update, super.getConfiguration()))
            .toCompletableFuture();
   }

   @Override
   public SyncStrongCounter sync() {
      return syncCounter;
   }

   @Override
   public CompletableFuture<Long> getAndSet(long value) {
      return dispatcher.execute(factory.newSetOperation(name, value, useConsistentHash()))
            .toCompletableFuture();
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

      @Override
      public long getAndSet(long value) {
         return await(StrongCounterImpl.this.getAndSet(value));
      }
   }
}
