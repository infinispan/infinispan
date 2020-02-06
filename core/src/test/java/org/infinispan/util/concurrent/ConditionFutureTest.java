package org.infinispan.util.concurrent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.commons.test.Exceptions.expectCompletionException;
import static org.infinispan.util.concurrent.CompletionStages.isCompletedSuccessfully;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.concurrent.ConditionFutureTest")
public class ConditionFutureTest extends AbstractInfinispanTest {
   ScheduledExecutorService timeoutExecutor =
         Executors.newSingleThreadScheduledExecutor(getTestThreadFactory("timeout"));

   @AfterClass(alwaysRun = true)
   public void tearDown() {
      timeoutExecutor.shutdownNow();
   }

   public void testBeforeFirstUpdate() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);
      CompletionStage<Void> stage = conditionFuture.newConditionStage(i -> i > 0, 10, SECONDS);
      assertFalse(stage.toCompletableFuture().isDone());

      conditionFuture.update(1);
      assertTrue(stage.toCompletableFuture().isDone());
   }

   public void testAlreadyCompleted() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);
      conditionFuture.update(1);
      CompletionStage<Void> stage = conditionFuture.newConditionStage(i -> i > 0, 10, SECONDS);
      assertTrue(stage.toCompletableFuture().isDone());
   }

   public void testConcurrentModification() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);
      CompletionStage<Void> stage11 = conditionFuture.newConditionStage(i -> i > 0, 10, SECONDS);
      CompletionStage<Void> stage12 = conditionFuture.newConditionStage(i -> i > 0, 10, SECONDS);

      // Block the completion of stage11
      CompletableFuture<Void> updateReleased = new CompletableFuture<>();
      stage11.thenRun(updateReleased::join);
      stage12.thenRun(updateReleased::join);

      // Update the condition future, triggering the completion of stage1x
      conditionFuture.updateAsync(1, testExecutor());
      eventually(() -> isCompletedSuccessfully(stage11) || isCompletedSuccessfully(stage12));

      // Add 2 new condition stages while the update is blocked, to increment modCount by 2
      CompletionStage<Void> stage21 = conditionFuture.newConditionStage(i -> i > 1, 10, SECONDS);
      CompletionStage<Void> stage22 = conditionFuture.newConditionStage(i -> i > 1, 10, SECONDS);

      // Unblock the condition future update
      updateReleased.complete(null);
      CompletionStages.join(stage11);
      CompletionStages.join(stage12);

      // Update again to complete stage21 and stage22
      conditionFuture.update(2);
      CompletionStages.join(stage21);
      CompletionStages.join(stage22);
   }

   public void testUpdateAsyncException() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);
      CompletionStage<Void> stage1 = conditionFuture.newConditionStage(i -> i > 0, 10, SECONDS);

      ExecutorService executor = Executors.newSingleThreadExecutor(getTestThreadFactory(""));
      executor.shutdown();

      conditionFuture.updateAsync(1, executor);

      expectCompletionException(RejectedExecutionException.class, stage1);
   }

   public void testStopException() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);

      CompletionStage<Void> stage = conditionFuture.newConditionStage(i -> i > 1, 10, SECONDS);
      assertFalse(stage.toCompletableFuture().isDone());

      conditionFuture.stop();

      expectCompletionException(IllegalLifecycleStateException.class, stage);
   }

   public void testDuplicatePredicate() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);

      Predicate<Integer> test = i -> i > 0;
      CompletionStage<Void> stage1 = conditionFuture.newConditionStage(test, 10, SECONDS);
      CompletionStage<Void> stage2 = conditionFuture.newConditionStage(test, 10, SECONDS);
      assertFalse(stage1.toCompletableFuture().isDone());
      assertFalse(stage2.toCompletableFuture().isDone());

      conditionFuture.update(1);
      assertTrue(stage1.toCompletableFuture().isDone());
      assertTrue(stage2.toCompletableFuture().isDone());
   }
}
