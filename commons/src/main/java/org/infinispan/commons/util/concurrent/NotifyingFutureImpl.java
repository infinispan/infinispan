package org.infinispan.commons.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Constructs an instance of a {@link org.infinispan.commons.util.concurrent.NotifyingFuture}.
 * <p/>
 * Typical usage:
 * <p/>
 * <pre><code>
 * Object retval = ... // do some work here
 * NotifyingFuture nf = new NotifyingFutureImpl();
 * rpcManager.broadcastRpcCommandInFuture(nf, command);
 * return nf;
 * </code></pre>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NotifyingFutureImpl<T> extends BaseNotifyingFuture<T> implements NotifyingNotifiableFuture<T>{

   private T actualReturnValue;
   private Throwable exceptionThrown;
   private volatile Future<T> future;
   private final CountDownLatch latch = new CountDownLatch(1);

   @Override
   public void setFuture(Future<T> future) {
      this.future = future;
      latch.countDown();
   }

   public Future<T> getFuture() throws InterruptedException {
      latch.await();
      return future;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      try {
         return getFuture().cancel(mayInterruptIfRunning);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   @Override
   public boolean isCancelled() {
      try {
         return getFuture().isCancelled();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   @Override
   public boolean isDone() {
      if (callCompleted) {
         return true;
      }
      try {
         return getFuture().isDone();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      if (!callCompleted) {
         getFuture().get();
      }
      if (exceptionThrown != null) {
         throw new ExecutionException(exceptionThrown);
      }
      return actualReturnValue;
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      if (!callCompleted) {
         getFuture().get(timeout, unit);
      }
      if (exceptionThrown != null) {
         throw new ExecutionException(exceptionThrown);
      }
      return actualReturnValue;
   }

   @Override
   public void notifyDone(T result) {
      actualReturnValue = result;
      fireListeners();
   }

   @Override
   public void notifyException(Throwable exception) {
      exceptionThrown = exception;
      fireListeners();
   }
}