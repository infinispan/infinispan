package org.infinispan.client.hotrod.impl.async;

import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Notifying future implementation for async calls.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NotifyingFutureImpl<T> implements NotifyingFuture<T> {

   private volatile Future<T> executing;
   private volatile CopyOnWriteArraySet<FutureListener<T>> listeners;

   public void setExecuting(Future<T> executing) {
      this.executing = executing;
   }

   @Override
   public NotifyingFuture<T> attachListener(FutureListener<T> futureListener) {
      if (listeners == null) {
         listeners = new CopyOnWriteArraySet<FutureListener<T>>();
      }
      listeners.add(futureListener);
      return this;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      try {
         return executing.cancel(mayInterruptIfRunning);
      } finally {
         notifyFutureCompletion();
      }
   }

   public void notifyFutureCompletion() {
      if (listeners != null) {
         for (FutureListener<T> listener : listeners) {
            listener.futureDone(this);
         }
      }
   }

   @Override
   public boolean isCancelled() {
      return executing.isCancelled();
   }

   @Override
   public boolean isDone() {
      return executing.isDone();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      return executing.get();
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return executing.get(timeout, unit);
   }
}
