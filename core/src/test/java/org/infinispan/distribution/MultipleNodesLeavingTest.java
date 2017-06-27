package org.infinispan.distribution;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "distribution.MultipleNodesLeavingTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC})
public class MultipleNodesLeavingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(cacheMode, false), 4);
      waitForClusterToForm();
   }

   public void testMultipleLeaves() throws Exception {

      //kill 3 caches at once
      fork(() -> manager(3).stop());
      fork(() -> manager(2).stop());
      fork(() -> manager(1).stop());

      eventuallyEquals(1, () -> advancedCache(0).getRpcManager().getTransport().getMembers().size());

      log.trace("MultipleNodesLeavingTest.testMultipleLeaves");

      TestingUtil.blockUntilViewsReceived(60000, false, cache(0));
      TestingUtil.waitForNoRebalance(cache(0));
      List<Address> caches = advancedCache(0).getDistributionManager().getWriteConsistentHash().getMembers();
      log.tracef("caches = %s", caches);
      int size = caches.size();
      assert size == 1;
   }
}
