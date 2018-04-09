package org.infinispan.counter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.util.StrongTestCounter;
import org.testng.annotations.Test;

/**
 * A simple consistency test for bounded {@link org.infinispan.counter.api.StrongCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.BoundedCounterTest")
public class BoundedCounterTest extends StrongCounterTest {

   public void testSimpleThreshold(Method method) {
      CounterManager counterManager = counterManager(0);
      counterManager.defineCounter(method.getName(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(-1).upperBound(1).build());
      StrongTestCounter counter = new StrongTestCounter(counterManager.getStrongCounter(method.getName()));
      addAndAssertResult(counter, 1, 1);
      assertOutOfBoundsAdd(counter, 1, 1);
      addAndAssertResult(counter, -1, 0);
      addAndAssertResult(counter, -1, -1);
      assertOutOfBoundsAdd(counter, -1, -1);

      counter.reset();

      assertOutOfBoundsAdd(counter, 2, 1);
      assertOutOfBoundsAdd(counter, -3, -1);
   }

   public void testCompareAndSetBounds(Method method) {
      CounterManager counterManager = counterManager(0);
      counterManager.defineCounter(method.getName(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(-2).upperBound(2).build());
      SyncStrongCounter counter = counterManager.getStrongCounter(method.getName()).sync();
      assertTrue(counter.compareAndSet(0, 2));
      assertEquals(2, counter.getValue());
      assertOutOfBoundCompareAndSet(counter, 2, 3);
      counter.reset();
      assertTrue(counter.compareAndSet(0, -2));
      assertEquals(-2, counter.getValue());
      assertOutOfBoundCompareAndSet(counter, -2, -3);
      counter.reset();
      assertFalse(counter.compareAndSet(1, 3));
      assertFalse(counter.compareAndSet(1, -3));
   }

   public void testCompareAndSwapBounds(Method method) {
      CounterManager counterManager = counterManager(0);
      counterManager.defineCounter(method.getName(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(-2).upperBound(2).build());
      SyncStrongCounter counter = counterManager.getStrongCounter(method.getName()).sync();
      assertEquals(0, counter.compareAndSwap(0, 2));
      assertEquals(2, counter.getValue());
      assertOutOfBoundCompareAndSwap(counter, 2, 3);
      counter.reset();
      assertEquals(0, counter.compareAndSwap(0, -2));
      assertEquals(-2, counter.getValue());
      assertOutOfBoundCompareAndSwap(counter, -2, -3);
      counter.reset();
      assertEquals(0, counter.compareAndSwap(1, 3));
      assertEquals(0, counter.compareAndSwap(1, -3));
   }

   @Test(groups = "unstable", description = "ISPN-8786")
   @Override
   public void testCompareAndSetConcurrent(Method method)
         throws ExecutionException, InterruptedException, TimeoutException {
      super.testCompareAndSetConcurrent(method);
   }

   @Override
   protected StrongTestCounter createCounter(CounterManager counterManager, String counterName, long initialValue) {
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(Long.MIN_VALUE)
                  .upperBound(Long.MAX_VALUE).initialValue(initialValue).build());
      return new StrongTestCounter(counterManager.getStrongCounter(counterName));
   }

   @Override
   protected void assertMaxValueAfterMaxValue(StrongTestCounter counter, long delta) {
      assertOutOfBoundsAdd(counter, delta, Long.MAX_VALUE);
   }

   @Override
   protected void assertMinValueAfterMinValue(StrongTestCounter counter, long delta) {
      assertOutOfBoundsAdd(counter, delta, Long.MIN_VALUE);
   }

   @Override
   protected List<CounterConfiguration> configurationToTest() {
      return Arrays.asList(
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(10).lowerBound(1).build(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(-10).upperBound(1).build(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(1).upperBound(2).upperBound(2)
                  .build(),
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).build()
      );
   }

   private void assertOutOfBoundsAdd(StrongTestCounter counter, long delta, long expected) {
      try {
         counter.add(delta);
         fail("Bound should have been reached!");
      } catch (CounterOutOfBoundsException e) {
         log.debug("Expected exception.", e);
      }
      assertEquals("Wrong return value of counter.getNewValue()", expected, counter.getValue());
   }

   private void assertOutOfBoundCompareAndSet(SyncStrongCounter counter, long expect, long value) {
      try {
         counter.compareAndSet(expect, value);
         fail("Threshold should have been reached!");
      } catch (CounterOutOfBoundsException e) {
         log.debug("Expected exception", e);
      }
      assertEquals("Wrong return value of counter.getNewValue()", expect, counter.getValue());
   }

   private void assertOutOfBoundCompareAndSwap(SyncStrongCounter counter, long expect, long value) {
      try {
         counter.compareAndSwap(expect, value);
         fail("Threshold should have been reached!");
      } catch (CounterOutOfBoundsException e) {
         log.debug("Expected exception", e);
      }
      assertEquals("Wrong return value of counter.getNewValue()", expect, counter.getValue());
   }
}
