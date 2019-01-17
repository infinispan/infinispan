package org.infinispan.counter;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.WeakCounter;
import org.testng.annotations.Test;

/**
 * A simple consistency test for {@link WeakCounter} with zero capacity nodes.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@Test(groups = "functional", testName = "counter.WeakCounterWithZeroCapacityNodesTest")
public class WeakCounterWithZeroCapacityNodesTest extends WeakCounterTest {

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(nodeId % 2 == 0);
   }
}
