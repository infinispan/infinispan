package org.infinispan.tx.totalorder.simple.dist;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.dist.SyncPrepareTotalOrderTest")
public class SyncPrepareTotalOrderTest extends BaseSimpleTotalOrderTest {

   public SyncPrepareTotalOrderTest() {
      this(3);
   }

   protected SyncPrepareTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.DIST_SYNC, false, false);
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return DistributionTestHelper.isOwner(cache, key);
   }
}
