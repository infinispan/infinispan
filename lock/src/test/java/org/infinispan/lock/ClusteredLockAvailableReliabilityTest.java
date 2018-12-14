package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.configuration.Reliability;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockAvailableReliabilityTest")
public class ClusteredLockAvailableReliabilityTest extends ClusteredLockTest {

   public ClusteredLockAvailableReliabilityTest() {
      super();
      reliability = Reliability.AVAILABLE;
      numOwner = 3;
      cacheMode = CacheMode.DIST_SYNC;
   }

   @Override
   protected int clusterSize() {
      return 6;
   }
}
