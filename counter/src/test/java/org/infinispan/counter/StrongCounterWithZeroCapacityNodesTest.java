package org.infinispan.counter;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * A simple consistency test for {@link org.infinispan.counter.api.StrongCounter} where some nodes are capacity factor
 * 0.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@Test(groups = "functional", testName = "counter.StrongCounterWithZeroCapacityNodesTest")
public class StrongCounterWithZeroCapacityNodesTest extends StrongCounterTest {

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(nodeId % 2 == 0);
   }
}
