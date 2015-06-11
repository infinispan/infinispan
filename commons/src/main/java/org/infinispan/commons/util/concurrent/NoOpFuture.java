package org.infinispan.commons.util.concurrent;

import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;

/**
 * A future that doesn't do anything and simply returns a given return value.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class NoOpFuture<E> implements NotifyingNotifiableFuture<E> {
   private final E returnValue;
   private final Throwable t;

   public NoOpFuture(E returnValue) {
      this.returnValue = returnValue;
      this.t = null;
   }

   public NoOpFuture(Throwable t) {
      this.returnValue = null;
      this.t = t;
   }

   @Override
   public boolean cancel(boolean b) {
      return false;
   }

   @Override
   public boolean isCancelled() {
      return false;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   @Override
   public E get() throws InterruptedException, ExecutionException {
      if (t != null) {
         throw new ExecutionException(t);
      }
      return returnValue;
   }

   @Override
   public E get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
      return get();
   }

   @Override
   public void notifyDone(E result) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void notifyException(Throwable exception) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setFuture(Future<E> eFuture) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NotifyingFuture<E> attachListener(FutureListener<E> eFutureListener) {
      eFutureListener.futureDone(this);
      return this;
   }
}
