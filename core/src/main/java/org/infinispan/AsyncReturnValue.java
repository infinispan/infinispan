package org.infinispan;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wraps up return values for the asunc API
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AsyncReturnValue implements Future<Object> {
   final Future<Object> networkCallFuture;
   final Object actualReturnValue;

   public AsyncReturnValue(Future<Object> networkCallFuture, Object actualReturnValue) {
      this.networkCallFuture = networkCallFuture;
      this.actualReturnValue = actualReturnValue;
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      return networkCallFuture.cancel(mayInterruptIfRunning);
   }

   public boolean isCancelled() {
      return networkCallFuture.isCancelled();
   }

   public boolean isDone() {
      return networkCallFuture.isDone();
   }

   public Object get() throws InterruptedException, ExecutionException {
      networkCallFuture.get();
      return actualReturnValue;
   }

   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      networkCallFuture.get(timeout, unit);
      return actualReturnValue;
   }
}
