package org.infinispan.server.hotrod.counter.impl;

import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_REMOVE;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_RESET;

import java.util.concurrent.CompletableFuture;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.server.hotrod.counter.op.CounterAddOp;
import org.infinispan.server.hotrod.counter.op.CounterOp;
import org.infinispan.server.hotrod.counter.response.CounterValueTestResponse;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link WeakCounter} implementation for Hot Rod server testing.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class TestWeakCounter implements WeakCounter {

   private final String name;
   private final CounterConfiguration configuration;
   private final HotRodClient client;
   private final TestCounterNotificationManager notificationManager;

   TestWeakCounter(String name, CounterConfiguration configuration, HotRodClient client,
         TestCounterNotificationManager notificationManager) {
      this.name = name;
      this.configuration = configuration;
      this.client = client;
      this.notificationManager = notificationManager;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public long getValue() {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_GET, name);
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      switch (response.getStatus()) {
         case Success:
            return ((CounterValueTestResponse) response).getValue();
         case ServerError:
            throw new CounterException(((TestErrorResponse) response).msg);
         default:
            throw new CounterException("unknown response " + response);
      }
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      CounterAddOp op = new CounterAddOp(client.protocolVersion(), name, delta);
      return executeOp(op);
   }

   @Override
   public CompletableFuture<Void> reset() {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_RESET, name);
      return executeOp(op);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.register(name, listener);
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public CompletableFuture<Void> remove() {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_REMOVE, name);
      client.writeOp(op);
      client.getResponse(op);
      return CompletableFutures.completedNull();
   }

   @Override
   public SyncWeakCounter sync() {
      throw new UnsupportedOperationException(); //no need for testing
   }

   private CompletableFuture<Void> executeOp(CounterOp op) {
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      CompletableFuture<Void> future = new CompletableFuture<>();
      switch (response.getStatus()) {
         case Success:
            future.complete(null);
            break;
         case OperationNotExecuted:
            future.complete(null);
            break;
         case KeyDoesNotExist:
            future.completeExceptionally(new IllegalStateException("Counter Not Found!"));
            break;
         case ServerError:
            future.completeExceptionally(new CounterException(((TestErrorResponse) response).msg));
            break;
         default:
            future.completeExceptionally(new Exception("unknown response " + response));
      }
      return future;
   }
}
