package org.infinispan.counter;

import static java.lang.String.format;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.util.StrongTestCounter;
import org.testng.annotations.Test;

/**
 * A simple consistency test.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.StrongCounterTest")
public class StrongCounterTest extends AbstractCounterTest<StrongTestCounter> {

   private static final int CLUSTER_SIZE = 4;

   public void testUniqueReturnValues(Method method) throws ExecutionException, InterruptedException, TimeoutException {
      final List<Future<List<Long>>> workers = new ArrayList<>(CLUSTER_SIZE);
      final String counterName = method.getName();
      final CyclicBarrier barrier = new CyclicBarrier(CLUSTER_SIZE);
      final long counterLimit = 1000;

      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         final int cmIndex = i;
         workers.add(fork(() -> {
            List<Long> retValues = new LinkedList<>();
            CounterManager manager = counterManager(cmIndex);
            StrongTestCounter counter = createCounter(manager, counterName, 0);
            long lastRet = 0;
            barrier.await();

            while (lastRet < counterLimit) {
               lastRet = counter.addAndGet(1);
               retValues.add(lastRet);
            }

            return retValues;
         }));
      }

      final Set<Long> uniqueValuesCheck = new HashSet<>();
      for (Future<List<Long>> w : workers) {
         List<Long> returnValues = w.get(1, TimeUnit.MINUTES);
         for (Long l : returnValues) {
            assertTrue(format("Duplicated value %d", l), uniqueValuesCheck.add(l));
         }
      }
      for (long l = 1; l < (counterLimit + 3); ++l) {
         assertTrue(format("Value %d does not exists!", l), uniqueValuesCheck.contains(l));
      }
   }

   public void testCompareAndSet(Method method) {
      final String counterName = method.getName();
      TestContext context = new TestContext();
      context.printSeed(counterName);

      long expect = context.random.nextLong();
      long value = context.random.nextLong();

      StrongTestCounter counter = createCounter(counterManager(0), counterName, expect);
      for (int i = 0; i < 10; ++i) {
         assertTrue(counter.compareAndSet(expect, value));
         assertEquals(value, counter.getValue());
         expect = value;
         value = context.random.nextLong();
      }

      for (int i = 0; i < 10; ++i) {
         long notExpected = context.random.nextLong();
         while (expect == notExpected) {
            notExpected = context.random.nextLong();
         }
         assertFalse(counter.compareAndSet(notExpected, value));
         assertEquals(expect, counter.getValue());
      }
   }

   public void testCompareAndSetMaxAndMinLong(Method method) {
      final String counterName = method.getName();
      StrongTestCounter counter = createCounter(counterManager(0), counterName, 0);
      assertFalse(counter.compareAndSet(-1, Long.MAX_VALUE));
      assertEquals(0, counter.getValue());
      assertTrue(counter.compareAndSet(0, Long.MAX_VALUE));
      assertEquals(Long.MAX_VALUE, counter.getValue());
      counter.reset();
      assertFalse(counter.compareAndSet(-1, Long.MIN_VALUE));
      assertEquals(0, counter.getValue());
      assertTrue(counter.compareAndSet(0, Long.MIN_VALUE));
      assertEquals(Long.MIN_VALUE, counter.getValue());
   }

   @Override
   protected StrongTestCounter createCounter(CounterManager counterManager, String counterName, long initialValue) {
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(initialValue).build());
      return new StrongTestCounter(counterManager.getStrongCounter(counterName));
   }

   @Override
   protected void assertMaxValueAfterMaxValue(StrongTestCounter counter, long delta) {
      assertEquals(Long.MAX_VALUE, counter.addAndGet(delta));
      assertEquals(Long.MAX_VALUE, counter.getValue());
   }

   @Override
   protected void addAndAssertResult(StrongTestCounter counter, long delta, long expected) {
      assertEquals(format("Wrong return value after adding %d", delta), expected, counter.addAndGet(delta));
      assertEquals("Wrong return value of counter.getNewValue()", expected, counter.getValue());
   }

   @Override
   protected void assertMinValueAfterMinValue(StrongTestCounter counter, long delta) {
      assertEquals(Long.MIN_VALUE, counter.addAndGet(delta));
      assertEquals(Long.MIN_VALUE, counter.getValue());
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }
}
