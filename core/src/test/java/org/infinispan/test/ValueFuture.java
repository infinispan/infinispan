package org.infinispan.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A simple <code>Future</code> implementation whose <code>get()</code> method blocks until another thread calls <code>set()</code>.
 *
 * @author Dan Berindei &lt;dberinde@redhat.com&gt;
 * @since 5.0
 */
public class ValueFuture<V> implements Future<V> {
   private CountDownLatch setLatch = new CountDownLatch(1);
   private V value;
   private Throwable exception;

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
      return false;
   }

   @Override
   public V get() throws InterruptedException, ExecutionException {
      setLatch.await();
      if (exception != null)
         throw new ExecutionException(exception);
      return value;
   }

   @Override
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      setLatch.await(timeout, unit);
      if (exception != null)
         throw new ExecutionException(exception);
      return value;
   }

   public void set(V value) {
      this.value = value;
      setLatch.countDown();
   }

   public void setException(Throwable exception) {
      this.exception = exception;
      setLatch.countDown();
   }
}
