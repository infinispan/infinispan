package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Testing query functionality on RAM directory for DIST_SYNC cache mode with enabled Topology aware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.TopologyAwareDistributedCacheTest")
public class TopologyAwareDistributedCacheTest extends TopologyAwareClusteredCacheTest {

   @Override
   public CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
