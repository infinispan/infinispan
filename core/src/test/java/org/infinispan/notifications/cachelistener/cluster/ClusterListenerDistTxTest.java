package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of transactional and dist
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistTxTest")
public class ClusterListenerDistTxTest extends AbstractClusterListenerTxTest {
   public ClusterListenerDistTxTest() {
      super(CacheMode.DIST_SYNC);
   }
}
