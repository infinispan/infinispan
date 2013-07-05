package org.infinispan.commons.util.concurrent;

import java.util.concurrent.*;

/**
 * An abstract Future that has "completed"
 *
 * @author Manik Surtani
 * @version 4.1
 */
public abstract class AbstractInProcessFuture<V> implements Future<V> {
   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
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
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      return get();
   }
}
