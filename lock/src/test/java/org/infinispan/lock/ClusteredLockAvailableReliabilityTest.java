package org.infinispan.lock;

import org.infinispan.lock.configuration.Reliability;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockAvailableReliabilityTest")
public class ClusteredLockAvailableReliabilityTest extends ClusteredLockTest {

   public ClusteredLockAvailableReliabilityTest() {
      super();
      reliability = Reliability.AVAILABLE;
      numOwner = 3;
   }

   @Override
   protected int clusterSize() {
      return 6;
   }
}
