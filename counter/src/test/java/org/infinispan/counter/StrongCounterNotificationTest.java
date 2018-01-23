package org.infinispan.counter;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.counter.util.TestCounter;
import org.testng.annotations.Test;

/**
 * Notification test for {@link org.infinispan.counter.api.StrongCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.StrongCounterNotificationTest")
public class StrongCounterNotificationTest extends AbstractCounterNotificationTest {
   @Override
   protected TestCounter createCounter(CounterManager counterManager, String counterName) {
      counterManager.defineCounter(counterName, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
      return new StrongTestCounter(counterManager.getStrongCounter(counterName));
   }
}
