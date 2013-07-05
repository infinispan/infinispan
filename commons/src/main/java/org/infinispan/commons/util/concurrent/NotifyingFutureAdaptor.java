package org.infinispan.commons.util.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Receives a {@link Future} and exposes it as an {@link NotifyingFuture}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NotifyingFutureAdaptor<T> extends BaseNotifyingFuture<T> {

   private static Log log = LogFactory.getLog(NotifyingFutureAdaptor.class);

   private volatile Future<T> actual;

   public void setActual(Future<T> actual) {
      this.actual = actual;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return actual.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return actual.isCancelled();
   }

   @Override
   public boolean isDone() {
      return actual.isDone();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      T result = actual.get();
      log.tracef("Actual future completed with result %s", result);
      return result;
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return actual.get(timeout, unit);
   }
}
