package org.infinispan.util.concurrent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.util.concurrent.CompletionStages.isCompletedSuccessfully;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
}
