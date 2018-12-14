package org.infinispan.lock.impl.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.configuration.Reliability;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockImplLowAvailableReliabilityTest")
public class ClusteredLockImplLowAvailableReliabilityTest extends ClusteredLockImplTest {

   public ClusteredLockImplLowAvailableReliabilityTest() {
      super();
      reliability = Reliability.AVAILABLE;
      numOwner = 1;
      cacheMode = CacheMode.DIST_SYNC;
   }

}
