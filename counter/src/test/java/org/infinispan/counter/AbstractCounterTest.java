package org.infinispan.counter;

import static java.lang.String.format;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.util.TestCounter;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = {"functional", "smoke"})
public abstract class AbstractCounterTest<T extends TestCounter> extends BaseCounterTest {

   public void testDiffInitValues(Method method) throws ExecutionException, InterruptedException {
      final TestContext context = new TestContext();
      final String counterName = method.getName();
      long initialValue = 0;

      context.printSeed(counterName);

      List<T> counters = new ArrayList<>(clusterSize());
      for (int i = 0; i < clusterSize(); ++i) {
         long rndLong = context.random.nextLong();
         if (i == 0) {
            initialValue = rndLong;
         }
         log.debug(context.message("StrongCounter #%d initial value is %d", i, rndLong));
         counters.add(createCounter(counterManager(i), counterName, rndLong));
      }

      for (int i = 0; i < clusterSize(); ++i) {
         final int index = i;
         eventuallyEquals(context.message("Wrong initial value for counter #%d", i), initialValue,
               () -> counters.get(index).getValue());
      }
   }

   public void testReset(Method method) throws ExecutionException, InterruptedException {
      final String counterName = method.getName();
      final TestContext context = new TestContext();
      context.printSeed(counterName);
      final long initialValue = context.random.nextLong();

      List<T> counters = new ArrayList<>(clusterSize());
      for (int i = 0; i < clusterSize(); ++i) {
         counters.add(createCounter(counterManager(i), counterName, initialValue));
      }

      for (int i = 0; i < clusterSize(); ++i) {
         long delta = context.random.nextLong();
         addIgnoringBounds(counters.get(i), delta);
         log.debug(context.message("StrongCounter #%d, Add %d", i, delta));
         counters.get(i).reset();
         final int index = i;
         eventuallyEquals(context.message("Wrong initial value for counter #%d", i), initialValue,
               () -> counters.get(index).getValue());

      }

      for (int i = 0; i < clusterSize(); ++i) {
         long delta = context.random.nextLong();
         addIgnoringBounds(counters.get(i), delta);
         log.debug(context.message("StrongCounter #%d, Add %d", i, delta));
         counters.get(0).reset();
         final int index = i;
         eventuallyEquals(context.message("Wrong initial value for counter #%d", i), initialValue,
               () -> counters.get(index).getValue());
      }
   }

   public void testMaxAndMinLong(Method method) throws ExecutionException, InterruptedException {
      final String counterName = method.getName();
      T counter = createCounter(counterManager(0), counterName, 0);

      addAndAssertResult(counter, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
      addAndAssertResult(counter, 1, Long.MAX_VALUE);

      assertMaxValueAfterMaxValue(counter, 1);
      assertMaxValueAfterMaxValue(counter, 1000);

      addAndAssertResult(counter, -1, Long.MAX_VALUE - 1);

      counter.reset();

      addAndAssertResult(counter, Long.MIN_VALUE + 1, Long.MIN_VALUE + 1);
      addAndAssertResult(counter, -1, Long.MIN_VALUE);

      assertMinValueAfterMinValue(counter, -1);
      assertMinValueAfterMinValue(counter, -1000);

      addAndAssertResult(counter, 1, Long.MIN_VALUE + 1);
   }

   protected abstract void assertMinValueAfterMinValue(T counter, long delta);

   protected abstract T createCounter(CounterManager counterManager, String counterName, long initialValue);

   protected abstract void assertMaxValueAfterMaxValue(T counter, long delta);

   protected abstract void addAndAssertResult(T counter, long delta, long expected);

   private void addIgnoringBounds(T counter, long delta) {
      try {
         counter.add(delta);
      } catch (CounterOutOfBoundsException e) {
         log.debug("ignored.", e);
      }
   }

   class TestContext {
      final Random random;
      private final long seed;

      TestContext() {
         this(System.nanoTime());
      }

      //in case we need to test with specific seed
      private TestContext(long seed) {
         this.seed = seed;
         this.random = new Random(seed);
      }

      void printSeed(String testName) {
         log.infof("Test '%s' seed is %d", testName, seed);
      }

      String message(String format, Object... args) {
         return "[seed=" + seed + "] " + format(format, args);
      }
   }

}
