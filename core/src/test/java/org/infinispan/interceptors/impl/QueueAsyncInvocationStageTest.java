package org.infinispan.interceptors.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;

import org.infinispan.interceptors.InvocationCallback;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "interceptors.QueueAsyncInvocationStageTest")
public class QueueAsyncInvocationStageTest extends AbstractInfinispanTest {
   @DataProvider(name = "offsets")
   public Object[][] offsets() {
      return new Object[][]{{0}, {1}};
   }

   @Test(dataProvider = "offsets")
   public void testExpandCapacity(int splitOffset) throws Throwable {
      CompletableFuture<Object> future = new CompletableFuture<>();
      QueueAsyncInvocationStage stage =
            new QueueAsyncInvocationStage(null, null, future, makeCallback(0));
      assertCallback(0, stage.queuePoll());
      addAndPoll(stage, splitOffset);

      // Now trigger 2 expansions
      int count = 2 * QueueAsyncInvocationStage.QUEUE_INITIAL_CAPACITY;
      addAndPoll(stage, count);
   }

   private InvocationCallback makeCallback(int i) {
      return (rCtx, rCommand, rv, throwable) -> "v" + i;
   }

   private void assertCallback(int index, InvocationCallback callback) throws Throwable {
      assertEquals("v" + index, callback.apply(null, null, null, null));
   }

   private void addAndPoll(QueueAsyncInvocationStage stage, int splitOffset) throws Throwable {
      for (int i = 0; i < splitOffset; i++) {
         stage.queueAdd(makeCallback(i));
      }
      assertEquals(splitOffset, stage.queueSize());
      for (int i = 0; i < splitOffset; i++) {
         assertCallback(i, stage.queuePoll());
      }
      assertEquals(0, stage.queueSize());
   }
}
