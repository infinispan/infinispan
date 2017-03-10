package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.infinispan.remoting.responses.Response;

class CountDownCompletableFuture extends CompletableFuture<Object> implements BiConsumer<Response, Throwable> {
   protected final AtomicInteger counter;

   CountDownCompletableFuture(int participants) {
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

   @Override
   public void accept(Response response, Throwable throwable) {
      if (throwable != null) {
         completeExceptionally(throwable);
      } else {
         countDown();
      }
   }
}
