package org.infinispan.commons.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For use with > 1 underlying network future
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AggregatingNotifyingFutureImpl<T> extends NotifyingFutureImpl<T> {
   final List<Future<T>> futures;
   final AtomicInteger awaitingCompletions = new AtomicInteger();

   public AggregatingNotifyingFutureImpl(T actualReturnValue, int maxFutures) {
      super(actualReturnValue);
      futures = new ArrayList<Future<T>>(maxFutures);
      awaitingCompletions.set(maxFutures);
   }

   @Override
   public void setNetworkFuture(Future<T> future) {
      futures.add(future);
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      boolean aggregateValue = false;
      for (Future<T> f : futures) aggregateValue = f.cancel(mayInterruptIfRunning) && aggregateValue;
      return aggregateValue;
   }

   @Override
   public boolean isCancelled() {
      for (Future<T> f : futures) if (f.isCancelled()) return true;
      return false;
   }

   @Override
   public boolean isDone() {
      for (Future<T> f : futures) if (!f.isDone()) return false;
      return true;
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      for (Future<T> f : futures) f.get();
      return actualReturnValue;
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      for (Future<T> f : futures) f.get(timeout, unit);
      return actualReturnValue;
   }

   @Override
   public void notifyDone() {
      if (awaitingCompletions.decrementAndGet() == 0) super.notifyDone();
   }
}
