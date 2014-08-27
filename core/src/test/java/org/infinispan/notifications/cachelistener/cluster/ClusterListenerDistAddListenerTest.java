package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of non tx and dist when adding listeners at inopportune times
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistAddListenerTest")
public class ClusterListenerDistAddListenerTest extends AbstractClusterListenerDistAddListenerTest {
   public ClusterListenerDistAddListenerTest() {
      super(false);
   }
}
