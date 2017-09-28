package org.infinispan.counter;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.counter.util.TestCounter;
import org.infinispan.counter.util.WeakTestCounter;
import org.testng.annotations.Test;

/**
 * A simple notification test for {@link WeakCounterImpl}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.WeakCounterNotificationTest")
public class WeakCounterNotificationTest extends AbstractCounterNotificationTest {

   protected TestCounter createCounter(CounterManager counterManager, String counterName) {
      counterManager.defineCounter(counterName, CounterConfiguration.builder(CounterType.WEAK).concurrencyLevel(4).build());
      return new WeakTestCounter(counterManager.getWeakCounter(counterName));
   }
}
