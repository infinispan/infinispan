package org.infinispan.distribution.ch;

import java.util.Collection;

import org.infinispan.remoting.transport.Address;
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
   protected int minPrimaryOwned(int numSegments, int numNodes) {
      return (int) (0.4 * super.minPrimaryOwned(numSegments, numNodes));
   }

   @Override
   protected int maxPrimaryOwned(int numSegments, int numNodes) {
      return (int) Math.ceil(2.5 * super.maxPrimaryOwned(numSegments, numNodes));
   }

   @Override
   protected int minOwned(int numSegments, int numNodes, int actualNumOwners) {
      return (int) (0.4 * super.minOwned(numSegments, numNodes, actualNumOwners));
   }

   @Override
   protected int maxOwned(int numSegments, int numNodes, int actualNumOwners) {
      return (int) Math.ceil(2.5 * super.maxOwned(numSegments, numNodes, actualNumOwners));
   }

   @Override
   protected int allowedMoves(int numSegments, int numOwners, Collection<Address> oldMembers,
                                 Collection<Address> newMembers) {
      int minMembers = Math.min(oldMembers.size(), newMembers.size());
      int diffMembers = symmetricalDiff(oldMembers, newMembers).size();
      return numSegments * numOwners * (diffMembers / minMembers + 1);
   }
}
