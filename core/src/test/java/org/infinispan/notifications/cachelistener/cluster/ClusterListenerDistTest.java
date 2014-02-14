package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of non tx and dist
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistTest")
public class ClusterListenerDistTest extends AbstractClusterListenerNonTxTest {
   public ClusterListenerDistTest() {
      super(false, CacheMode.DIST_SYNC);
   }
}
