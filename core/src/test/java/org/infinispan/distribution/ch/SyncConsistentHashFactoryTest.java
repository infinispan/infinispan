package org.infinispan.distribution.ch;

import org.testng.annotations.Test;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link SyncConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.SyncConsistentHashFactoryTest")
public class SyncConsistentHashFactoryTest extends DefaultConsistentHashFactoryTest {
   @Override
   protected ConsistentHashFactory createConsistentHashFactory() {
      return new SyncConsistentHashFactory();
   }

   // Disclaimer: These numbers just happen to work with our test addresses, they are by no means guaranteed
   // by the SyncConsistentHashFactory algorithm. In theory it could trade stability of segments on join/leave
   // in order to guarantee a better distribution, but I haven't done anything in that area yet.
   @Override
   protected float allowedDeviationPrimaryOwned(int numSegments, int numNodes, float totalCapacity, float maxCapacityFactor) {
      return 1.25f * numSegments * maxCapacityFactor / totalCapacity;
   }

   @Override
   protected float allowedDeviationOwned(int numSegments, int actualNumOwners, int numNodes, float totalCapacity,
                                          float maxCapacityFactor) {
//      if (numSegments < numNodes)
//         return numSegments;

      return 1.25f * actualNumOwners * numSegments * maxCapacityFactor / totalCapacity;
   }

   @Override
   protected int allowedExtraMoves(DefaultConsistentHash oldCH, DefaultConsistentHash newCH, int leaverSegments) {
      int minMembers = Math.min(oldCH.getMembers().size(), newCH.getMembers().size());
      int diffMembers = symmetricalDiff(oldCH.getMembers(), newCH.getMembers()).size();
      return oldCH.getNumSegments() * oldCH.getNumOwners() * (diffMembers / minMembers + 1);
   }
}
