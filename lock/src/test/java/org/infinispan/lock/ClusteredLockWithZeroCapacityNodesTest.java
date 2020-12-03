package org.infinispan.lock;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWithZeroCapacityNodesTest")
public class ClusteredLockWithZeroCapacityNodesTest extends ClusteredLockTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            // REPL
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(-1),
            // DIST
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(1),
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(9),
            };
   }

   protected int clusterSize() {
      return 3;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return super.configure(nodeId).zeroCapacityNode(nodeId % 2 == 1);
   }
}
