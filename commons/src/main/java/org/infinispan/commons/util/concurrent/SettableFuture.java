package org.infinispan.commons.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simple future whose value can be set.
 */
public class SettableFuture<V> implements Future<V> {
   CountDownLatch latch = new CountDownLatch(1);
   V value;
   Throwable t;
   boolean done = false;

   @Override
   public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      return false;
   }

   @Override
   public synchronized boolean isCancelled() {
      return false;
   }

   @Override
   public synchronized boolean isDone() {
      return done;
   }

   @Override
   public V get() throws InterruptedException, ExecutionException {
      synchronized (this) {
         if (done) {
            if (t != null) throw new ExecutionException(t);
            return value;
         }
      }
      latch.await();
      synchronized (this) {
         if (t != null) throw new ExecutionException(t);
         return value;
      }
   }

   @Override
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      synchronized (this) {
         if (done) {
            if (t != null) throw new ExecutionException(t);
            return value;
         }
      }
      if (!latch.await(timeout, unit)) {
         throw new TimeoutException();
      }
      synchronized (this) {
         if (t != null) throw new ExecutionException(t);
         return value;
      }
   }

   public synchronized void set(V value) {
      if (done) {
         throw new IllegalStateException("Already set!");
      } else {
         this.value = value;
         latch.countDown();
      }
   }

   public synchronized void setThrowable(Throwable t) {
      if (done) {
         throw new IllegalStateException("Already set!");
      } else {
         this.t = t;
         latch.countDown();
      }
   }
}
