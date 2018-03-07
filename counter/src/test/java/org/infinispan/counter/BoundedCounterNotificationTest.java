package org.infinispan.counter;

import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.counter.util.TestCounter;
import org.testng.annotations.Test;

/**
 * Notification test for threshold aware counters.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.BoundedCounterNotificationTest")
public class BoundedCounterNotificationTest extends AbstractCounterNotificationTest {

   public void testThreshold(Method method) throws Exception {
      final String counterName = method.getName();
      final StrongCounter[] counters = new StrongCounter[clusterSize()];
      for (int i = 0; i < clusterSize(); ++i) {
         counters[i] = createCounter(counterManager(i), counterName, -2, 2);
      }

      Handle<ListenerQueue> l = counters[0].addListener(new ListenerQueue(counterName));

      if (counters.length != CLUSTER_SIZE) {
         for (int i = 0; i < CLUSTER_SIZE; ++i) {
            counters[0].incrementAndGet();
         }
      } else {
         for (StrongCounter counter : counters) {
            counter.incrementAndGet();
         }
      }

      l.getCounterListener().assertEvent(0, CounterState.VALID, 1, CounterState.VALID);
      l.getCounterListener().assertEvent(1, CounterState.VALID, 2, CounterState.VALID);
      l.getCounterListener().assertEvent(2, CounterState.VALID, 2, CounterState.UPPER_BOUND_REACHED);

      //as soon as threshold is reached, no more events are triggered.
      assertTrue(l.getCounterListener().queue.isEmpty());

      eventuallyEquals(2L, () -> counters[0].sync().getValue());

      if (counters.length != CLUSTER_SIZE) {
         for (int i = 0; i < CLUSTER_SIZE; ++i) {
            counters[0].decrementAndGet();
         }
      } else {
         for (StrongCounter counter : counters) {
            counter.decrementAndGet();
         }
      }

      l.getCounterListener().assertEvent(2, CounterState.UPPER_BOUND_REACHED, 1, CounterState.VALID);
      l.getCounterListener().assertEvent(1, CounterState.VALID, 0, CounterState.VALID);
      l.getCounterListener().assertEvent(0, CounterState.VALID, -1, CounterState.VALID);
      l.getCounterListener().assertEvent(-1, CounterState.VALID, -2, CounterState.VALID);
      eventuallyEquals(-2L, () -> counters[0].sync().getValue());

      counters[0].decrementAndGet();
      counters[0].decrementAndGet();

      l.getCounterListener().assertEvent(-2, CounterState.VALID, -2, CounterState.LOWER_BOUND_REACHED);
      //as soon as threshold is reached, no more events are triggered.
      assertTrue(l.getCounterListener().queue.isEmpty());
      eventuallyEquals(-2L, () -> counters[0].sync().getValue());

      //removes the listener
      l.remove();

      counters[0].incrementAndGet();
      counters[0].incrementAndGet();

      assertTrue(l.getCounterListener().queue.isEmpty());
   }

   protected TestCounter createCounter(CounterManager counterManager, String counterName) {
      return new StrongTestCounter(createCounter(counterManager, counterName, Long.MIN_VALUE, Long.MAX_VALUE));
   }

   private StrongCounter createCounter(CounterManager counterManager, String counterName, long min, long max) {
      counterManager.defineCounter(counterName,
            CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(min).upperBound(max).build());
      return counterManager.getStrongCounter(counterName);
   }
}
