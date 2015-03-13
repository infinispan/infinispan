package org.infinispan.commons.util.concurrent;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Utilities for {@link org.infinispan.commons.util.concurrent.NotifyingFuture} composition.
 *
 * @author gustavonalle
 * @since 7.1
 */
public final class Futures {

   private Futures() {
   }

   /**
    * Returns a new {@link org.infinispan.commons.util.concurrent.NotifyingFuture} that will be completed when all of
    * the given futures completes.
    *
    * @param futures List of NotifyingFutures
    * @return a new composite NotifyingFuture
    */
   public static <T> NotifyingFuture<List<T>> combine(final List<NotifyingFuture<T>> futures) {
      if (futures == null || futures.isEmpty()) return new NoOpFuture<>(null);
      return new CompositeNotifyingFuture<>(futures);
   }

   /**
    * @see {@link Futures#andThen(NotifyingFuture, Runnable, java.util.concurrent.ExecutorService)}
    */
   public static <T> NotifyingFuture<Void> andThen(final NotifyingFuture<T> future, final Runnable after) {
      return andThen(future, after, Executors.newSingleThreadExecutor());
   }

   /**
    * Executes a task asynchronously after a future completion
    *
    * @param future          NotifyingFuture input
    * @param after           Runnable to be executed after the input future completion
    * @param executorService The executor in order to run the task
    * @return NotifyingFuture with the result
    */
   public static <T> NotifyingFuture<Void> andThen(final NotifyingFuture<T> future, final Runnable after, final ExecutorService executorService) {
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<>();
      final CountDownLatch countDownLatch = new CountDownLatch(1);
      Future afterFuture = executorService.submit(new Runnable() {
         @Override
         public void run() {
            try {
               countDownLatch.await();
               future.get();
               after.run();
               result.notifyDone(null);
            } catch (InterruptedException e) {
               future.cancel(true);
               Thread.currentThread().interrupt();
            } catch (Exception e) {
               result.notifyException(e);
            }
         }
      });
      result.setFuture(afterFuture);
      FutureListener<T> listener = new FutureListener<T>() {
         @Override
         public void futureDone(Future<T> future) {
            countDownLatch.countDown();
         }
      };
      future.attachListener(listener);
      return result;
   }
}
