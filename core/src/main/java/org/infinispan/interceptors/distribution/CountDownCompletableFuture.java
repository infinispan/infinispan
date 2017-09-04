package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class CountDownCompletableFuture extends CompletableFuture<Object> {
   protected final AtomicInteger counter;

   public CountDownCompletableFuture(int participants) {
      this.counter = new AtomicInteger(participants);
      assert participants != 0;
   }

   public void countDown() {
      if (counter.decrementAndGet() == 0) {
         Object result = null;
         try {
            result = result();
         } catch (Throwable t) {
            completeExceptionally(t);
         } finally {
            // no-op when completed with exception
            complete(result);
         }
      }
   }

   public void increment() {
      int preValue = counter.getAndIncrement();
      if (preValue == 0) {
         throw new IllegalStateException();
      }
   }

   protected Object result() {
      return null;
   }
}
