package org.infinispan.util;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestException;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit Tests for {@link ActionSequencer}
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "unit", testName = "util.ActionSequencerUnitTest")
public class ActionSequencerUnitTest extends AbstractInfinispanTest {

   private static void assertEmpty(ActionSequencer sequencer) {
      assertMapSize(sequencer, 0);
      assertPendingActions(sequencer, 0);
   }

   private static int nextInt() {
      return ThreadLocalRandom.current().nextInt();
   }

   private static String nextStringInt() {
      return Integer.toString(nextInt());
   }

   private static int getResult(CompletionStage<Integer> cf) {
      return CompletionStages.join(cf);
   }

   private static void assertMapSize(ActionSequencer sequencer, int size) {
      assertEquals("Wrong ActionSequencer.getMapSize()", size, sequencer.getMapSize());
   }

   private static void assertPendingActions(ActionSequencer sequencer, int size) {
      assertEquals("Wrong ActionSequencer.getPendingActions()", size, sequencer.getPendingActions());
   }

   private static void assertActionResult(CompletionStage<Integer> cf, int result) {
      assertEquals("Wrong result", result, getResult(cf));
   }

   private static void assertActionResult(CompletionStage<Integer> cf, String exceptionMessage) {
      expectException(CompletionException.class, TestException.class, exceptionMessage,
            () -> CompletionStages.join(cf));
   }

   private static void assertActionState(NonBlockingAction action, CompletionStage<Integer> cf, boolean started,
         boolean completed) {
      assertEquals("Is action started?", started, action.isStarted());
      assertEquals("Is action completed?", completed, cf.toCompletableFuture().isDone());
   }

   private static void assertActionState(List<NonBlockingAction> actionList, List<CompletionStage<Integer>> cfList,
         Predicate<Integer> started, Predicate<Integer> completed) {
      for (int i = 0; i < actionList.size(); ++i) {
         assertActionState(actionList.get(i), cfList.get(i), started.test(i), completed.test(i));
      }
   }

   private static void assertAllCompleted(int[] results, List<CompletionStage<Integer>> cfList,
         Predicate<Integer> fail) {
      for (int i = 0; i < results.length; ++i) {
         if (fail.test(i)) {
            assertActionResult(cfList.get(i), Integer.toString(results[i]));
         } else {
            assertActionResult(cfList.get(i), results[i]);
         }
      }
   }

   @DataProvider(name = "default-with-keys")
   public static Object[][] dataProviderWithKeys() {
      return new Object[][]{
            {KeysSupplier.NO_KEY},
            {KeysSupplier.SINGLE_KEY},
            {KeysSupplier.MULTIPLE_KEY}
      };
   }

   @Test(dataProvider = "default-with-keys")
   public void testExecution(KeysSupplier keysSupplier) {
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);
      int retVal = nextInt();
      Collection<Object> keys = keysSupplier.get();
      NonBlockingAction action = new NonBlockingAction(retVal);
      CompletionStage<Integer> cf = keysSupplier.useSingleKeyMethod() ?
                                    sequencer.orderOnKey(keys.iterator().next(), action) :
                                    sequencer.orderOnKeys(keys, action);

      assertPendingActions(sequencer, keys.isEmpty() ? 0 : 1);
      assertMapSize(sequencer, keys.size());

      action.awaitUntilStarted();
      assertActionState(action, cf, true, false);
      action.continueExecution();

      assertActionResult(cf, retVal);
      assertEmpty(sequencer);
   }

   public void testNullParameters() {
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);
      NonBlockingAction action = new NonBlockingAction(0);

      expectException(NullPointerException.class, () -> sequencer.orderOnKeys(asList("k1", "k2"), null));
      expectException(NullPointerException.class, () -> sequencer.orderOnKeys(null, action));

      expectException(NullPointerException.class, () -> sequencer.orderOnKey("k0", null));
      expectException(NullPointerException.class, () -> sequencer.orderOnKey(null, action));

      assertEmpty(sequencer);
   }

   @Test(dataProvider = "default-with-keys")
   public void testExceptionExecution(KeysSupplier keysSupplier) {
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);
      Collection<Object> keys = keysSupplier.get();
      String msg = nextStringInt();
      NonBlockingAction action = new NonBlockingAction(new TestException(msg));
      CompletionStage<Integer> cf = keysSupplier.useSingleKeyMethod() ?
                                    sequencer.orderOnKey(keys.iterator().next(), action) :
                                    sequencer.orderOnKeys(keys, action);

      assertPendingActions(sequencer, keys.isEmpty() ? 0 : 1);
      assertMapSize(sequencer, keys.size());

      action.awaitUntilStarted();
      assertActionState(action, cf, true, false);
      action.continueExecution();

      assertActionResult(cf, msg);
      assertEmpty(sequencer);
   }

   public void testSingleKeyOrder() {
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);
      Collection<Object> keys = singleton("k");

      int[] results = new int[3];
      List<NonBlockingAction> actionList = new ArrayList<>(results.length);
      List<CompletionStage<Integer>> cfList = new ArrayList<>(results.length);

      for (int i = 0; i < results.length; ++i) {
         createAndOrderAction(sequencer, results, actionList, cfList, keys, i, i == 1);
      }

      assertPendingActions(sequencer, results.length);
      assertMapSize(sequencer, keys.size());

      actionList.get(0).awaitUntilStarted(); //only the first is allowed to start!
      assertActionState(actionList, cfList, i -> i == 0, i -> false);

      //first is completed, the second should start
      actionList.get(0).continueExecution();
      actionList.get(1).awaitUntilStarted();
      assertActionResult(cfList.get(0), results[0]);
      assertActionState(actionList, cfList, i -> i <= 1, i -> i == 0);

      //allowing the last task to continue won't finish second task
      actionList.get(2).continueExecution();
      assertActionState(actionList, cfList, i -> i <= 1, i -> i == 0);

      //everything should be completed!
      actionList.get(1).continueExecution();
      actionList.get(2).awaitUntilStarted();

      assertAllCompleted(results, cfList, i -> i == 1);
      assertEmpty(sequencer);
   }

   public void testDistinctKeysWithSameKey() {
      doDistinctKeysTest(asList("k1", "k1", "k1"), 1);
   }

   public void testDistinctKeys() {
      doDistinctKeysTest(asList("k1", "k2", "k2"), 2);
   }

   public void testMultiKeyOrder() {
      //test:
      // * T1
      // * (T2 and T3) depends on T1 but they can run in parallel
      // * T4 depends on T2 and T3
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);

      int[] results = new int[4];
      List<NonBlockingAction> actionList = new ArrayList<>(results.length);
      List<CompletionStage<Integer>> cfList = new ArrayList<>(results.length);

      //T1
      createAndOrderAction(sequencer, results, actionList, cfList, asList("k1", "k2", "k3"), 0, false);

      //T2 (fail action) and T3
      createAndOrderAction(sequencer, results, actionList, cfList, singleton("k1"), 1, true);
      createAndOrderAction(sequencer, results, actionList, cfList, asList("k3", "k4"), 2, false);

      //T4
      createAndOrderAction(sequencer, results, actionList, cfList, asList("k1", "k4"), 3, false);

      assertPendingActions(sequencer, results.length);
      assertMapSize(sequencer, 4);

      //initial state. only T1 is started
      actionList.get(0).awaitUntilStarted();
      assertActionState(actionList, cfList, i -> i == 0, i -> false);

      //T1 is completed, T2 and T3 must be started
      actionList.get(0).continueExecution();
      actionList.get(1).awaitUntilStarted();
      actionList.get(2).awaitUntilStarted();
      assertActionResult(cfList.get(0), results[0]);
      assertActionState(actionList, cfList, i -> i <= 2, i -> i == 0);

      //T2 is finished but T3 isn't. T4 should be blocked!
      actionList.get(1).continueExecution();
      assertActionResult(cfList.get(1), Integer.toString(results[1]));
      assertActionState(actionList, cfList, i -> i <= 2, i -> i <= 1);

      //T3 is finished. T4 is started
      actionList.get(2).continueExecution();
      actionList.get(3).awaitUntilStarted();
      assertActionResult(cfList.get(2), results[2]);
      assertActionState(actionList, cfList, i -> i <= 3, i -> i <= 2);

      //everything is finished!
      actionList.get(3).continueExecution();
      assertActionResult(cfList.get(3), results[3]);
      assertEmpty(sequencer);
   }

   private void createAndOrderAction(ActionSequencer sequencer,
         int[] results, List<NonBlockingAction> actionList, List<CompletionStage<Integer>> cfList,
         Collection<Object> keys,
         int index, boolean fail) {
      results[index] = nextInt();
      NonBlockingAction action = fail ? new NonBlockingAction(new TestException(Integer.toString(results[index])))
                                      : new NonBlockingAction(results[index]);
      actionList.add(action);
      cfList.add(sequencer.orderOnKeys(keys, action));
   }

   private void doDistinctKeysTest(Collection<Object> keys, int distinctKeys) {
      ActionSequencer sequencer = new ActionSequencer(testExecutor(), false, TIME_SERVICE);
      sequencer.setStatisticEnabled(true);
      int retVal = ThreadLocalRandom.current().nextInt();
      NonBlockingAction action = new NonBlockingAction(retVal);
      CompletionStage<Integer> cf = sequencer.orderOnKeys(keys, action);

      assertPendingActions(sequencer, 1);
      assertMapSize(sequencer, distinctKeys);

      action.continueExecution();

      assertActionResult(cf, retVal);
      assertEmpty(sequencer);
   }

   private enum KeysSupplier implements Supplier<Collection<Object>> {
      NO_KEY(emptyList()),
      SINGLE_KEY(singleton("k1")),
      SINGLE_KEY_WITH_SINGLE_METHOD(singleton("k1"), true),
      MULTIPLE_KEY(asList("k2", "k3", "k4"));

      final Collection<Object> keys;
      final boolean useSingleKeyMethod;

      KeysSupplier(Collection<Object> keys) {
         this(keys, false);
      }

      KeysSupplier(Collection<Object> keys, boolean useSingleKeyMethod) {
         this.keys = keys;
         this.useSingleKeyMethod = useSingleKeyMethod;
      }

      @Override
      public Collection<Object> get() {
         return keys;
      }

      boolean useSingleKeyMethod() {
         return useSingleKeyMethod;
      }
   }

   private static class NonBlockingAction implements Callable<CompletableFuture<Integer>> {

      private final Integer retVal;
      private final Exception throwable;
      private final CompletableFuture<Integer> cf;
      private final CountDownLatch beforeLatch = new CountDownLatch(1);

      private NonBlockingAction(int retVal) {
         this.retVal = retVal;
         this.throwable = null;
         this.cf = new CompletableFuture<>();
      }

      private NonBlockingAction(Exception throwable) {
         this.retVal = null;
         this.throwable = throwable;
         this.cf = new CompletableFuture<>();
      }

      @Override
      public CompletableFuture<Integer> call() throws Exception {
         beforeLatch.countDown();
         return cf;
      }

      void continueExecution() {
         if (throwable != null) {
            cf.completeExceptionally(throwable);
         } else {
            cf.complete(retVal);
         }
      }

      boolean isStarted() {
         return beforeLatch.getCount() == 0;
      }

      void awaitUntilStarted() {
         try {
            if (beforeLatch.await(10, TimeUnit.SECONDS)) {
               return;
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
         fail("Action never started! action=" + this);
      }
   }
}
