package org.infinispan.notifications.cachelistener.cluster;

import org.testng.annotations.Test;

/**
 * Cluster listener test to make sure that when include current state is enabled that listeners still work properly
 * in a distributed transactional cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistTxInitialStateTest")
public class ClusterListenerDistTxInitialStateTest extends ClusterListenerDistTxTest {
   @Override
   protected ClusterListener listener() {
      return new ClusterListenerWithIncludeCurrentState();
   }
}
