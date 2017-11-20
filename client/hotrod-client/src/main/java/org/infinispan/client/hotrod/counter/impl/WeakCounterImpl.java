package org.infinispan.client.hotrod.counter.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A {@link WeakCounter} implementation for Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class WeakCounterImpl implements WeakCounter {

   private final String name;
   private final CounterConfiguration configuration;
   private final ExecutorService executorService;
   private final CounterHelper counterHelper;
   private final SyncWeakCounter syncCounter;
   private final NotificationManager notificationManager;

   WeakCounterImpl(String name, CounterConfiguration configuration, ExecutorService executorService,
         CounterHelper counterHelper, NotificationManager notificationManager) {
      this.name = name;
      this.configuration = configuration;
      this.executorService = executorService;
      this.counterHelper = counterHelper;
      this.notificationManager = notificationManager;
      syncCounter = new Sync();
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public long getValue() {
      return counterHelper.getValue(name);
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      return CompletableFuture.runAsync(() -> syncCounter.add(delta), executorService);
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
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public CompletableFuture<Void> remove() {
      return CompletableFuture.runAsync(syncCounter::remove, executorService);
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
         return counterHelper.getValue(name);
      }

      @Override
      public void add(long delta) {
         counterHelper.addAndGet(name, delta);
      }

      @Override
      public void reset() {
         counterHelper.reset(name);
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
