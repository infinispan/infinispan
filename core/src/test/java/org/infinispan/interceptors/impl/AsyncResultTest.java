package org.infinispan.interceptors.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "interceptors.AsyncResultTest")
public class AsyncResultTest extends AbstractInfinispanTest {
   private static final AsyncResult.Invoker0 INVOKER0 = (callback, returnValue, throwable) -> null;
   private static final AsyncResult.Invoker1 INVOKER1 = (callback, p1, returnValue, throwable) -> null;
   private static final AsyncResult.Invoker2 INVOKER2 = (callback, p1, p2, returnValue, throwable) -> null;
   private static final int INVOKER1_SIZE = 3;

   @DataProvider(name = "offsets")
   public Object[][] offsets() {
      return new Object[][]{{0}, {1}};
   }

   @Test(dataProvider = "offsets")
   public void testExpandCapacity(int splitOffset) {
      AsyncResult result = new AsyncResult();
      addAndPoll(result, splitOffset);

      // Now trigger 2 expansions
      int count = 2 * AsyncResult.QUEUE_INITIAL_CAPACITY / INVOKER1_SIZE;
      addAndPoll(result, count);
   }

   private void addAndPoll(AsyncResult result, int splitOffset) {
      for (int i = 0; i < splitOffset; i++) {
         String si = String.valueOf(i);
         result.queueAdd(INVOKER1, si + "0", si + "1");
      }
      assertEquals(INVOKER1_SIZE * splitOffset, result.queueSize());
      for (int i = 0; i < splitOffset; i++) {
         String si = String.valueOf(i);

         assertEquals(INVOKER1, result.queuePoll());
         assertEquals(si + "0", result.queuePoll());
         assertEquals(si + "1", result.queuePoll());
      }
      assertEquals(0, result.queueSize());
   }
}
