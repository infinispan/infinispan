package org.infinispan.commons.util.concurrent;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This future covers several {@link NotifyingFuture notifying futures} - the results will be provided
 * and listeners will be fired as soon as all futures complete. This is accomplished by registering
 * a countdown decremented with completion listener on each of these futures.
 */
public class GroupingNotifyingFuture<T> extends BaseNotifyingFuture<Map<NotifyingFuture<T>, T>> {
   private final Collection<NotifyingFuture<T>> futures;
   private final AtomicInteger futuresRunning;
   private final CountDownLatch latch;

   public GroupingNotifyingFuture(Collection<NotifyingFuture<T>> futures) {
      this.futures = futures;
      latch = new CountDownLatch(1);
      futuresRunning = new AtomicInteger(futures.size());
      FutureListener<T> completionListener = new FutureListener<T>() {
         @Override
         public void futureDone(Future<T> future) {
            if (futuresRunning.decrementAndGet() == 0) {
               latch.countDown();
               fireListeners();
            }
         }
      };
      for (NotifyingFuture<T> future : futures) {
         future.attachListener(completionListener);
      }
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      boolean result = true;
      for (NotifyingFuture<T> future : futures) {
         result = result && future.cancel(mayInterruptIfRunning);
      }
      return result;
   }

   @Override
   public boolean isCancelled() {
      boolean result = true;
      for (NotifyingFuture<T> future : futures) {
         result = result && future.isCancelled();
      }
      return result;
   }

   @Override
   public boolean isDone() {
      boolean result = true;
      for (NotifyingFuture<T> future : futures) {
         result = result && future.isDone();
      }
      return result;
   }

   @Override
   public Map<NotifyingFuture<T>, T> get() throws InterruptedException, ExecutionException {
      latch.await();
      return innerGet();
   }

   @Override
   public Map<NotifyingFuture<T>, T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (!latch.await(timeout, unit)) {
         throw new TimeoutException();
      }
      return innerGet();
   }

   private Map<NotifyingFuture<T>, T> innerGet() throws InterruptedException, ExecutionException {
      Map<NotifyingFuture<T>, T> result = new HashMap<NotifyingFuture<T>, T>(futures.size());
      for (NotifyingFuture<T> future : futures) {
         result.put(future, future.get());
      }
      return result;
   }
}
