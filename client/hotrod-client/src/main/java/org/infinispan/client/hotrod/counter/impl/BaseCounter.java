package org.infinispan.client.hotrod.counter.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;

public class BaseCounter {
   protected final String name;
   protected final CounterConfiguration configuration;
   protected final CounterOperationFactory factory;
   protected final OperationDispatcher dispatcher;
   private final NotificationManager notificationManager;

   BaseCounter(CounterConfiguration configuration, String name, CounterOperationFactory factory,
               OperationDispatcher dispatcher, NotificationManager notificationManager) {
      this.configuration = configuration;
      this.name = name;
      this.factory = factory;
      this.dispatcher = dispatcher;
      this.notificationManager = notificationManager;
   }

   public String getName() {
      return name;
   }

   public CompletableFuture<Void> reset() {
      return dispatcher.execute(factory.newResetOperation(name, useConsistentHash()))
            .toCompletableFuture();
   }

   public CompletableFuture<Void> remove() {
      return dispatcher.execute(factory.newRemoveOperation(name, useConsistentHash()))
            .toCompletableFuture();
   }

   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.addListener(name, listener);
   }

   boolean useConsistentHash() {
      return false;
   }
}
