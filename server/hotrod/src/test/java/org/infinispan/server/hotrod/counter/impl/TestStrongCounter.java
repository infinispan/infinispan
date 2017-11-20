package org.infinispan.server.hotrod.counter.impl;

import static java.lang.String.format;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.FORMAT_MESSAGE;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.LOWER_BOUND;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.UPPER_BOUND;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_REMOVE;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_RESET;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.server.hotrod.counter.op.CounterAddOp;
import org.infinispan.server.hotrod.counter.op.CounterCompareAndSwapOp;
import org.infinispan.server.hotrod.counter.op.CounterOp;
import org.infinispan.server.hotrod.counter.response.CounterValueTestResponse;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link StrongCounter} implementation for Hot Rod server testing.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class TestStrongCounter implements StrongCounter {

   private final String name;
   private final CounterConfiguration configuration;
   private final HotRodClient client;
   private final TestCounterNotificationManager notificationManager;

   TestStrongCounter(String name, CounterConfiguration configuration, HotRodClient client,
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
   public CompletableFuture<Long> getValue() {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_GET, name);
      return executeOp(op, this::handleValue, null);
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      CounterAddOp op = new CounterAddOp(client.protocolVersion(), name, delta);
      return executeOp(op, this::handleValue, () -> delta > 0);
   }

   @Override
   public CompletableFuture<Void> reset() {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_RESET, name);
      return executeOp(op, this::handleVoid, null);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.register(name, listener);
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      CounterCompareAndSwapOp op = new CounterCompareAndSwapOp(client.protocolVersion(), name, expect, update);
      return executeOp(op, this::handleValue, () -> update >= configuration.upperBound());
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
   public SyncStrongCounter sync() {
      throw new UnsupportedOperationException(); //no need for testing
   }

   private void handleValue(CompletableFuture<Long> future, TestResponse response) {
      future.complete(((CounterValueTestResponse) response).getValue());
   }

   private void handleVoid(CompletableFuture<Void> future, @SuppressWarnings("unused") TestResponse unused) {
      future.complete(null);
   }

   private <T> CompletableFuture<T> executeOp(CounterOp op,
         BiConsumer<CompletableFuture<T>, TestResponse> responseHandler, BooleanSupplier canReachUpperBound) {
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      CompletableFuture<T> future = new CompletableFuture<>();
      switch (response.getStatus()) {
         case Success:
            responseHandler.accept(future, response);
            break;
         case OperationNotExecuted:
            responseHandler.accept(future, response);
            break;
         case KeyDoesNotExist:
            future.completeExceptionally(new IllegalStateException("Counter Not Found!"));
            break;
         case ServerError:
            future.completeExceptionally(new CounterException(((TestErrorResponse) response).msg));
            break;
         case NotExecutedWithPrevious:
            future.completeExceptionally(canReachUpperBound.getAsBoolean() ?
                                         new CounterOutOfBoundsException(format(FORMAT_MESSAGE, UPPER_BOUND)) :
                                         new CounterOutOfBoundsException(format(FORMAT_MESSAGE, LOWER_BOUND))
            );
            break;
         default:
            future.completeExceptionally(new Exception("unknown response " + response));
      }
      return future;
   }
}
