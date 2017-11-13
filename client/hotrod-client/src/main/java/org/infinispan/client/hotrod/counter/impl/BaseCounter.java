package org.infinispan.client.hotrod.counter.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;

public class BaseCounter {
   protected final String name;
   protected final CounterConfiguration configuration;
   protected final CounterOperationFactory factory;
   protected final NotificationManager notificationManager;

   public BaseCounter(CounterConfiguration configuration, String name, CounterOperationFactory factory,
                      NotificationManager notificationManager) {
      this.configuration = configuration;
      this.name = name;
      this.factory = factory;
      this.notificationManager = notificationManager;
   }

   public String getName() {
      return name;
   }

   public CompletableFuture<Void> reset() {
      return factory.newResetOperation(name).execute();
   }

   public CompletableFuture<Void> remove() {
      return factory.newRemoveOperation(name).execute();
   }

   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.addListener(name, listener);
   }
}
