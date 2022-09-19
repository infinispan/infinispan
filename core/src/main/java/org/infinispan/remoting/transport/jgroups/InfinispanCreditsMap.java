package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.jgroups.util.CreditMap;

import net.jcip.annotations.GuardedBy;

/**
 * TODO! document this
 */
public class InfinispanCreditsMap extends CreditMap {

   @GuardedBy("lock")
   private CompletionStage<Void> queue;

   public InfinispanCreditsMap(long credits) {
      super(credits);
   }

   public boolean nonBlockingDecrementIfEnoughCredits(int credits) {
      lock.lock();
      try {
         return !done && decrement(credits);
      } finally {
         lock.unlock();
      }
   }

   public void queue(Runnable runnable, Executor executor) {
      lock.lock();
      try {
         if (queue == null) {
            queue = CompletableFuture.runAsync(runnable, executor);
         } else {
            queue = queue.exceptionally(throwable -> null).thenRunAsync(runnable, executor);
         }
      } finally {
         lock.unlock();
      }
   }
}
