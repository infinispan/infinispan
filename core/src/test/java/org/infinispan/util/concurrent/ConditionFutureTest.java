package org.infinispan.util.concurrent;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
      CompletionStage<Void> stage = conditionFuture.newConditionStage(i -> i > 0, 1, TimeUnit.SECONDS);
      assertFalse(stage.toCompletableFuture().isDone());

      conditionFuture.update(1);
      assertTrue(stage.toCompletableFuture().isDone());
   }

   public void testAlreadyCompleted() {
      ConditionFuture<Integer> conditionFuture = new ConditionFuture<>(timeoutExecutor);
      conditionFuture.update(1);
      CompletionStage<Void> stage = conditionFuture.newConditionStage(i -> i > 0, 1, TimeUnit.SECONDS);
      assertTrue(stage.toCompletableFuture().isDone());
   }
}
