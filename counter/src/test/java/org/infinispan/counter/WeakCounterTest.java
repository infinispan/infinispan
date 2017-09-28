package org.infinispan.counter;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ExecutionException;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.util.WeakTestCounter;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.WeakCounterTest")
public class WeakCounterTest extends AbstractCounterTest<WeakTestCounter> {

   private static final int CLUSTER_SIZE = 4;

   public void testSingleConcurrencyLevel() throws ExecutionException, InterruptedException {
      final CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(manager(0));
      final String counterName = "c1-counter";
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.WEAK).concurrencyLevel(1).build());
      WeakCounter wc = counterManager.getWeakCounter(counterName);
      wc.add(2).get();
      assertEquals(2, wc.getValue());
   }

   @Override
   protected void assertMaxValueAfterMaxValue(WeakTestCounter counter, long delta) {
      counter.add(delta);
      eventuallyEquals(Long.MAX_VALUE, counter::getValue);
   }

   @Override
   protected void addAndAssertResult(WeakTestCounter counter, long delta, long expected) {
      counter.add(delta);
      eventuallyEquals(expected, counter::getValue);
   }

   @Override
   protected void assertMinValueAfterMinValue(WeakTestCounter counter, long delta) {
      counter.add(delta);
      eventuallyEquals(Long.MIN_VALUE, counter::getValue);
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   @Override
   protected WeakTestCounter createCounter(CounterManager counterManager, String counterName, long initialValue) {
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.WEAK).initialValue(initialValue).concurrencyLevel(4).build());
      return new WeakTestCounter(counterManager.getWeakCounter(counterName));
   }
}
