package org.infinispan.remoting.inboundhandler;

import static org.testng.AssertJUnit.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

final class BlockHandlerImpl<T> implements BlockHandler, Predicate<T> {

   private final Predicate<T> predicate;
   private final CountDownLatch commandBlockedLatch = new CountDownLatch(1);
   private final CountDownLatch commandFinishedLatch = new CountDownLatch(1);
   private final CompletableFuture<Void> afterBlocked = new CompletableFuture<>();

   BlockHandlerImpl(Predicate<T> predicate) {
      this.predicate = predicate;
   }

   @Override
   public boolean isBlocked() {
      return commandBlockedLatch.getCount() > 0;
   }

   @Override
   public void awaitUntilBlocked(Duration timeout) throws InterruptedException {
      assertTrue("Timeout waiting for the command to block",
            commandBlockedLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public void awaitUntilCommandCompleted(Duration duration) throws InterruptedException {
      assertTrue(commandFinishedLatch.await(duration.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public void unblock() {
      afterBlocked.complete(null);
   }

   @Override
   public boolean test(T t) {
      return predicate.test(t);
   }

   void runAfterBlocked(Runnable action) {
      commandBlockedLatch.countDown();
      afterBlocked.thenRunAsync(action, ForkJoinPool.commonPool()).thenRun(commandFinishedLatch::countDown);
   }
}
