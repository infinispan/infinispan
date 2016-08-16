package org.infinispan.tx.totalorder.simple.dist;

import static org.testng.Assert.assertFalse;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.dist.SyncPrepareWriteSkewTotalOrderTest")
public class SyncPrepareWriteSkewTotalOrderTest extends BaseSimpleTotalOrderTest {

   public SyncPrepareWriteSkewTotalOrderTest() {
      this(3);
   }

   protected SyncPrepareWriteSkewTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.DIST_SYNC, false, true, false);
   }

   @Override
   public final void testSinglePhaseTotalOrder() {
      assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return DistributionTestHelper.isOwner(cache, key);
   }
}
