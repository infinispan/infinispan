package org.infinispan.commons.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * When the underlying future fails, allows to try to compute the result again.
 * Listeners are fired and result value is produced only after the future is
 * considered successful.
 * The check&retry policy is determined by {@link Checker}. The logic can be
 * written as:
 * <code>
 *    NotifyingFuture f = ...;
 *    start: when (f.isDone):
 *       if (checker.check(f)) {
 *          fireRetriableFuture();
 *       } else {
 *          f = checker.retry();
 *          goto start;
 *       }
 * </code>
 */
public class RetriableNotifyingFuture<T> extends BaseNotifyingFuture<T> {
   private NotifyingFuture<T> future;
   private Checker<T> checker;
   private boolean done;
   private final CountDownLatch latch = new CountDownLatch(1);
   private Throwable checkerException;

   /**
    * When any method throws an (unchecked) exception, no further attempts are made and RetriableNotifyingFuture.get()
    * throws ExecutionException wrapping the thrown exception.
    * @param <T>
    */
   public interface Checker<T> {
      /**
       * @param future
       * @return True if the future is considered successful and no more retries are requested.
       */
      boolean check(Future<T> future);

      /**
       * If this method returns null, no more retries are attempted and the last future result is returned.
       *
       * @return New future retrying the operation.
       */
      NotifyingFuture<T> retry();
   }

   public RetriableNotifyingFuture(NotifyingFuture<T> future, Checker<T> checker) {
      if (future == null || checker == null) throw new NullPointerException();
      this.future = future;
      this.checker = checker;
      future.attachListener(new FutureListener<T>() {
         @Override
         public void futureDone(Future<T> future) {
            RetriableNotifyingFuture self = RetriableNotifyingFuture.this;
            synchronized (self) {
               try {
                  if (self.checker.check(future) || self.future.isCancelled()) {
                     notifyDone();
                  } else {
                     self.future = self.checker.retry();
                     if (self.future == null) {
                        notifyDone();
                     } else {
                        self.future.attachListener(this);
                     }
                  }
               } catch (Throwable t) {
                  checkerException = t;
                  notifyDone();
               }
            }
         }
      });
   }

   private void notifyDone() {
      done = true;
      latch.countDown();
      fireListeners();
   }

   @Override
   public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      if (future == null) return false;
      return future.cancel(mayInterruptIfRunning);
   }

   @Override
   public synchronized boolean isCancelled() {
      if (future == null) return false;
      return future.isCancelled();
   }

   @Override
   public synchronized boolean isDone() {
      return done;
   }

   private synchronized T innerGet() throws ExecutionException, InterruptedException {
      if (checkerException != null) {
         throw new ExecutionException("Checker has thrown an exception", checkerException);
      } else if (future == null) {
         throw new ExecutionException(new NullPointerException("null future"));
      }
      return future.get();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      latch.await();
      return innerGet();
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (!latch.await(timeout, unit)) throw new TimeoutException();
      return innerGet();
   }
}
