package org.infinispan.commons.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CompositeNotifyingFuture aggregates several NotifyingFuture and completes when all of them complete.
 *
 * @author gustavonalle
 * @since 7.1
 */
public final class CompositeNotifyingFuture<T> extends NotifyingFutureImpl<List<T>> {

   private final CountDownLatch remaining;
   private final List<NotifyingFuture<T>> futures;
   private volatile boolean cancelled = false;
   private List<T> results = new ArrayList<>();

   public CompositeNotifyingFuture(List<NotifyingFuture<T>> futures) {
      this.futures = futures;
      this.remaining = new CountDownLatch(futures.size());
      InternalListener internalListener = new InternalListener();
      for (NotifyingFuture<T> future : futures) {
         future.attachListener(internalListener);
      }
   }

   @Override
   public boolean isDone() {
      return remaining.getCount() == 0;
   }

   @Override
   public boolean isCancelled() {
      return cancelled;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      this.cancelled = true;
      boolean wasCancelled = true;
      for (NotifyingFuture<?> future : futures) {
         wasCancelled &= future.cancel(mayInterruptIfRunning);
      }
      return wasCancelled;
   }

   @Override
   public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (unit == null) throw new IllegalArgumentException("provided unit is null");
      if (cancelled) throw new CancellationException();
      if (!remaining.await(timeout, unit)) throw new TimeoutException();
      return super.get();
   }

   @Override
   public List<T> get() throws InterruptedException, ExecutionException {
      if (cancelled) throw new CancellationException();
      remaining.await();
      return super.get();
   }

   final class InternalListener implements FutureListener<T> {
      @Override
      public void futureDone(Future<T> future) {
         synchronized (this) {
            Throwable error = null;
            try {
               results.add(future.get());
            } catch (Throwable e) {
               error = e;
            }
            remaining.countDown();
            if (remaining.getCount() == 0) {
               if (error != null) {
                  if (error instanceof ExecutionException) {
                     notifyException(((ExecutionException)error).getCause());
                  } else {
                     notifyException(error);
                  }
               } else {
                  notifyDone(results);
               }
               setFuture(new NoOpFuture<>(results));
            }
         }

      }
   }

}
