package org.infinispan.remoting.transport;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.impl.Request;
import org.infinispan.remoting.transport.impl.RequestRepository;

/**
 * A remote invocation request.
 *
 * <p>Thread-safety: This class and its sub-classes are thread-safe. They use the {@code ResponseCollector}'s monitor
 * for synchronization, so that collectors usually don't need any explicit synchronization.</p>
 *
 * @author Dan Berindei
 * @since 9.1
 */
public abstract class AbstractRequest<T> extends CompletableFuture<T> implements Callable<Void>, Request<T> {
   protected final ResponseCollector<T> responseCollector;
   protected final long requestId;
   protected final RequestRepository repository;

   private volatile Future<?> timeoutFuture = null;

   protected AbstractRequest(long requestId, ResponseCollector<T> responseCollector, RequestRepository repository) {
      this.responseCollector = responseCollector;
      this.repository = repository;
      this.requestId = requestId;
   }

   /**
    * Called when the timeout task scheduled with {@link #setTimeout(ScheduledExecutorService, long, TimeUnit)} expires.
    */
   protected abstract void onTimeout();

   @Override
   public final long getRequestId() {
      return requestId;
   }

   /**
    * Schedule a timeout task on the given executor, and complete the request with a {@link
    * org.infinispan.util.concurrent.TimeoutException}
    * when the task runs.
    *
    * If a timeout task was already registered with this request, it is cancelled.
    */
   public void setTimeout(ScheduledExecutorService timeoutExecutor, long timeout, TimeUnit unit) {
      cancelTimeoutTask();
      ScheduledFuture<Void> timeoutFuture = timeoutExecutor.schedule(this, timeout, unit);
      setTimeoutFuture(timeoutFuture);
   }

   public void cancel(Exception exception) {
      completeExceptionally(exception);
   }

   // Override complete(), completeExceptionally(), and cancel() to cancel the timeout task and remove the request from the map
   @Override
   public boolean complete(T value) {
      cancelTimeoutTask();
      repository.removeRequest(requestId);
      return super.complete(value);
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      cancelTimeoutTask();
      repository.removeRequest(requestId);
      return super.completeExceptionally(ex);
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      cancelTimeoutTask();
      repository.removeRequest(requestId);
      return super.cancel(mayInterruptIfRunning);
   }

   // Implement Callable for the timeout task
   @Override
   public final Void call() throws Exception {
      onTimeout();
      return null;
   }

   private void setTimeoutFuture(Future<?> timeoutFuture) {
      this.timeoutFuture = timeoutFuture;
      if (isDone()) {
         timeoutFuture.cancel(false);
      }
   }

   private void cancelTimeoutTask() {
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
   }
}
