package org.infinispan.distribution.topologyaware;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyInfoBroadcastNoRehashTest")
public class TopologyInfoBroadcastNoRehashTest extends TopologyInfoBroadcastTest {

   @Override
   protected ConfigurationBuilder getClusterConfig() {
      ConfigurationBuilder configuration = super.getClusterConfig();
      configuration.clustering().stateTransfer().fetchInMemoryState(false);
      return configuration;
   }
}
