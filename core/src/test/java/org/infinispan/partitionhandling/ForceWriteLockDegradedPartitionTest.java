package org.infinispan.partitionhandling;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.ForceWriteLockDegradedPartitionTest")
public class ForceWriteLockDegradedPartitionTest extends BasePartitionHandlingTest {
   @Override
   public Object[] factory() {
      return new Object[] {
            new ForceWriteLockDegradedPartitionTest().partitionHandling(PartitionHandling.ALLOW_READS),
            new ForceWriteLockDegradedPartitionTest().partitionHandling(PartitionHandling.DENY_READ_WRITES)
      };
   }

   public ForceWriteLockDegradedPartitionTest() {
      numberOfOwners = 2;
      numMembersInCluster = 2;
   }

   public void testGetWithForceWriteLock() {
      PartitionDescriptor p0 = new PartitionDescriptor(0);
      PartitionDescriptor p1 = new PartitionDescriptor(1);
      String key = "key";
      cache(0).put(key, 0);
      splitCluster(p0, p1);
      partition(0).assertDegradedMode();
      partition(0).assertExceptionWithForceLock(key);
      partition(1).assertDegradedMode();
      partition(1).assertExceptionWithForceLock(key);
   }
}
