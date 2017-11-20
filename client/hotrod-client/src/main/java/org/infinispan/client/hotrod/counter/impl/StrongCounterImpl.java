package org.infinispan.client.hotrod.counter.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;

/**
 * A {@link StrongCounter} implementation for Hot Rod clients.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class StrongCounterImpl implements StrongCounter {

   private final String name;
   private final CounterConfiguration configuration;
   private final ExecutorService executorService;
   private final CounterHelper counterHelper;
   private final SyncStrongCounter syncCounter;
   private final NotificationManager notificationManager;

   StrongCounterImpl(String name, CounterConfiguration configuration, ExecutorService executorService,
         CounterHelper counterHelper, NotificationManager notificationManager) {
      this.name = name;
      this.configuration = configuration;
      this.executorService = executorService;
      this.counterHelper = counterHelper;
      this.notificationManager = notificationManager;
      this.syncCounter = new Sync();
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public CompletableFuture<Long> getValue() {
      return CompletableFuture.supplyAsync(syncCounter::getValue, executorService);
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      return CompletableFuture.supplyAsync(() -> syncCounter.addAndGet(delta), executorService);
   }

   @Override
   public CompletableFuture<Void> reset() {
      return CompletableFuture.runAsync(syncCounter::reset, executorService);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.addListener(name, listener);
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return CompletableFuture.supplyAsync(() -> syncCounter.compareAndSwap(expect, update), executorService);
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public CompletableFuture<Void> remove() {
      return CompletableFuture.runAsync(syncCounter::remove, executorService);
   }

   @Override
   public SyncStrongCounter sync() {
      return syncCounter;
   }

   private class Sync implements SyncStrongCounter {

      @Override
      public long addAndGet(long delta) {
         return counterHelper.addAndGet(name, delta);
      }

      @Override
      public void reset() {
         counterHelper.reset(name);
      }

      @Override
      public long getValue() {
         return counterHelper.getValue(name);
      }

      @Override
      public long compareAndSwap(long expect, long update) {
         return counterHelper.compareAndSwap(name, expect, update, configuration);
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
         counterHelper.remove(name);
      }
   }
}
