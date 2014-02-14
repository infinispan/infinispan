package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of transactional and replication
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerReplTxTest")
public class ClusterListenerReplTxTest extends AbstractClusterListenerTxTest {
   public ClusterListenerReplTxTest() {
      super(CacheMode.REPL_SYNC);
   }
}
