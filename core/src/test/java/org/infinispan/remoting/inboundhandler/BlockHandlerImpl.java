package org.infinispan.remoting.inboundhandler;

import static org.testng.AssertJUnit.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public final class BlockHandlerImpl<T> implements BlockHandler, Predicate<T> {

   private final Predicate<T> predicate;
   private final CountDownLatch blockedLatch = new CountDownLatch(1);
   private final CountDownLatch finishedLatch = new CountDownLatch(1);
   private final CompletableFuture<Void> blockingFuture = new CompletableFuture<>();

   public BlockHandlerImpl(Predicate<T> predicate) {
      this.predicate = predicate;
   }

   @Override
   public boolean isBlocked() {
      return blockedLatch.getCount() > 0;
   }

   @Override
   public void awaitUntilBlocked(Duration timeout) throws InterruptedException {
      assertTrue("Timeout waiting for the command to block",
            blockedLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public void awaitUntilCommandCompleted(Duration duration) throws InterruptedException {
      assertTrue(finishedLatch.await(duration.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public void unblock() {
      blockingFuture.complete(null);
   }

   @Override
   public boolean test(T t) {
      return predicate.test(t);
   }

   void runAfterBlocked(Runnable action) {
      onBlocked();
      blockingFuture.thenRunAsync(action, ForkJoinPool.commonPool()).thenRun(this::onFinished);
   }

   public CompletableFuture<Void> blockingFuture() {
      return blockingFuture;
   }

   public void onBlocked() {
      blockedLatch.countDown();
   }

   public void onFinished() {
      finishedLatch.countDown();
   }
}
