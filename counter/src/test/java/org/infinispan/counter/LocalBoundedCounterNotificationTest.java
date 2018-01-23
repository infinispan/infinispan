package org.infinispan.counter;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Notification test for threshold aware local counters.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "counter.LocalBoundedCounterTest")
public class LocalBoundedCounterNotificationTest extends BoundedCounterNotificationTest {

   @Override
   protected int clusterSize() {
      return 1;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return new GlobalConfigurationBuilder();
   }
}
