package org.infinispan.notifications.cachelistener.cluster;

import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of non tx and dist when adding listeners at inopportune times
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistTxAddListenerTest")
public class ClusterListenerDistTxAddListenerTest extends AbstractClusterListenerDistAddListenerTest {
   public ClusterListenerDistTxAddListenerTest() {
      super(true);
   }
}
