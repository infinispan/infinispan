package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWithZeroCapacityNodesTest")
public class ClusteredLockWithZeroCapacityNodesTest extends ClusteredLockTest {

   public ClusteredLockWithZeroCapacityNodesTest() {
      numOwner = 1;
      cacheMode = CacheMode.DIST_SYNC;
   }

   protected int clusterSize() {
      return 3;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return super.configure(nodeId).zeroCapacityNode(nodeId % 2 == 1);
   }
}
