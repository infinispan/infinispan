package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Cluster listener test having a configuration of non tx and replication
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerReplTest")
public class ClusterListenerReplTest extends AbstractClusterListenerNonTxTest {
   public ClusterListenerReplTest() {
      super(false, CacheMode.REPL_SYNC);
   }
}
