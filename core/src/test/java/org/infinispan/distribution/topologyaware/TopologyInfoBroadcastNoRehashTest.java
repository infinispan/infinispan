package org.infinispan.distribution.topologyaware;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.TopologyInfoBroadcastNoRehashTest")
public class TopologyInfoBroadcastNoRehashTest extends TopologyInfoBroadcastTest {

   @Override
   protected Configuration getClusterConfig() {
      Configuration configuration = super.getClusterConfig();
      configuration.setRehashEnabled(false);
      return configuration;
   }
}
