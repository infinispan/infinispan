package org.infinispan.distribution;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A version of the async return values for dist
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistAsyncReturnValue implements Future<Object> {
   final Future<Object> invalFuture, replFuture;
   final Object returnValue;

   public DistAsyncReturnValue(Future<Object> invalFuture, Future<Object> replFuture, Object returnValue) {
      this.invalFuture = invalFuture;
      this.replFuture = replFuture;
      this.returnValue = returnValue;
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      boolean invalCancelled = true;
      if (invalFuture != null) invalCancelled = invalFuture.cancel(mayInterruptIfRunning);
      return replFuture.cancel(mayInterruptIfRunning) && invalCancelled;
   }

   public boolean isCancelled() {
      return replFuture.isCancelled() && (invalFuture == null || invalFuture.isCancelled());
   }

   public boolean isDone() {
      return replFuture.isDone() && (invalFuture == null || invalFuture.isDone());
   }

   public Object get() throws InterruptedException, ExecutionException {
      if (invalFuture != null) invalFuture.get();
      replFuture.get();
      return returnValue;
   }

   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (invalFuture != null) invalFuture.get(timeout, unit);
      replFuture.get(timeout, unit);
      return returnValue;
   }
}
