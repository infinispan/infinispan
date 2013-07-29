package org.infinispan.commons.util.concurrent;

import java.util.concurrent.*;

/**
 * This is a notifying and notifiable future whose return value is not known
 * at construction time. Instead, the return value comes from the result of
 * the operation called in the Callable or Runnable.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class DeferredReturnFuture<V> implements NotifyingNotifiableFuture<V> {

   private final NotifyingFutureImpl<Object> delegateFuture = new NotifyingFutureImpl<Object>(null);

   @Override
   @SuppressWarnings("unchecked")
   public V get() throws InterruptedException, ExecutionException {
      // Return the network's future result
      return (V) delegateFuture.ioFuture.get();
   }

   @Override
   @SuppressWarnings("unchecked")
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      return (V) delegateFuture.ioFuture.get(timeout, unit);
   }

   @Override
   public void notifyDone() {
      delegateFuture.notifyDone();
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setNetworkFuture(Future<V> future) {
      delegateFuture.setNetworkFuture((Future<Object>) future);
   }

   @Override
   @SuppressWarnings("unchecked")
   public NotifyingFuture<V> attachListener(FutureListener<V> objectFutureListener) {
      return (NotifyingFuture<V>) delegateFuture.attachListener((FutureListener<Object>) objectFutureListener);
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return delegateFuture.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return delegateFuture.isCancelled();
   }

   @Override
   public boolean isDone() {
      return delegateFuture.isDone();
   }

}
