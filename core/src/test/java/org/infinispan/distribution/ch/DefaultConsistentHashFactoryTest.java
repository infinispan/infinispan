package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link DefaultConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.DefaultConsistentHashFactoryTest")
public class DefaultConsistentHashFactoryTest extends AbstractInfinispanTest {

   private int iterationCount = 0;

   protected ConsistentHashFactory createConsistentHashFactory() {
      return new DefaultConsistentHashFactory();
   }

   public void testConsistentHashDistribution() {
      int[] numSegments = {1, 2, 4, 8, 16, 50, 100, 500};
      int[] numNodes = {1, 2, 3, 4, 5, 7, 10, 100};
      int[] numOwners = {1, 2, 3, 5};
      // Since the number of nodes changes, the capacity factors are repeated
      float[][] capacityFactors = {null, {1}, {2}, {1, 100}, {2, 0, 1}};

      ConsistentHashFactory <DefaultConsistentHash> chf = createConsistentHashFactory();
      Hash hashFunction = new MurmurHash3();

      for (int nn : numNodes) {
         List<Address> nodes = new ArrayList<Address>(nn);
         for (int j = 0; j < nn; j++) {
            nodes.add(new TestAddress(j, "TA"));
         }

         for (int ns : numSegments) {
            if (nn < ns) {
               for (int no : numOwners) {
                  for (float[] lf : capacityFactors) {
                     Map<Address, Float> lfMap = null;
                     if (lf != null) {
                        lfMap = new HashMap<Address, Float>();
                        for (int i = 0; i < nn; i++) {
                           lfMap.put(nodes.get(i), lf[i % lf.length]);
                        }
                     }
                     testConsistentHashModifications(chf, hashFunction, nodes, ns, no, lfMap);
                  }
               }
            }
         }
      }
   }

   private void testConsistentHashModifications(ConsistentHashFactory<DefaultConsistentHash> chf, Hash hashFunction, List<Address> nodes, int ns, int no, Map<Address, Float> lfMap) {
      DefaultConsistentHash baseCH = chf.create(hashFunction, no, ns, nodes, lfMap);
      assertEquals(lfMap, baseCH.getCapacityFactors());
      checkDistribution(baseCH, lfMap, false);

      // each element in the array is a pair of numbers: the first is the number of nodes to add
      // the second is the number of nodes to remove (the index of the removed nodes are pseudo-random)
      int[][] nodeChanges = {{1, 0}, {2, 0}, {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {10, 0}, {0, 10}};

      // check that the base CH is already balanced
      List<Address> baseMembers = baseCH.getMembers();
      assertSame(baseCH, chf.updateMembers(baseCH, baseMembers, lfMap));
      assertSame(baseCH, chf.rebalance(baseCH));

      // starting point, so that we don't confuse nodes
      int nodeIndex = baseMembers.size();

      for (int i = 0; i < nodeChanges.length; i++) {
         int nodesToAdd = nodeChanges[i][0];
         int nodesToRemove = nodeChanges[i][1];
         if (nodesToRemove > baseMembers.size())
            break;
         if (nodesToRemove == baseMembers.size() && nodesToAdd == 0)
            break;

         List<Address> newMembers = new ArrayList<Address>(baseMembers);
         HashMap<Address, Float> newCapacityFactors = lfMap != null ?
               new HashMap<Address, Float>(lfMap) : null;
         for (int k = 0; k < nodesToRemove; k++) {
            int indexToRemove = Math.abs(baseCH.getHashFunction().hash(k) % newMembers.size());
            if (newCapacityFactors != null) {
               newCapacityFactors.remove(newMembers.get(indexToRemove));
            }
            newMembers.remove(indexToRemove);
         }
         for (int k = 0; k < nodesToAdd; k++) {
            TestAddress address = new TestAddress(nodeIndex++, "TA");
            newMembers.add(address);
            if (newCapacityFactors != null) {
               newCapacityFactors.put(address, lfMap.get(baseMembers.get(k % baseMembers.size())));
            }
         }

         log.tracef("Testing consistent hash modifications iteration %d. Initial CH is %s. New members are %s",
               iterationCount, baseCH, newMembers);
         baseCH = checkModificationsIteration(chf, baseCH, nodesToAdd, nodesToRemove, newMembers, newCapacityFactors);

         iterationCount++;
      }
   }

   private DefaultConsistentHash checkModificationsIteration(ConsistentHashFactory<DefaultConsistentHash> chf,
                                                             DefaultConsistentHash baseCH, int nodesToAdd,
                                                             int nodesToRemove, List<Address> newMembers,
                                                             Map<Address, Float> lfMap) {
      int actualNumOwners = computeActualNumOwners(baseCH.getNumOwners(), newMembers, lfMap);

      // first phase: just update the members list, removing the leavers
      // and adding new owners, but not necessarily assigning segments to them
      DefaultConsistentHash updatedMembersCH = chf.updateMembers(baseCH, newMembers, lfMap);
      assertEquals(lfMap, updatedMembersCH.getCapacityFactors());
      if (nodesToRemove > 0) {
         for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
            assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() > 0);
            assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() <= actualNumOwners);
         }
      }

      // second phase: rebalance with the new members list
      DefaultConsistentHash rebalancedCH = chf.rebalance(updatedMembersCH);
      checkDistribution(rebalancedCH, lfMap, false);

      for (int l = 0; l < rebalancedCH.getNumSegments(); l++) {
         assertTrue(rebalancedCH.locateOwnersForSegment(l).size() >= actualNumOwners);
      }

      checkMovedSegments(baseCH, rebalancedCH);

      // union doesn't have to keep the CH balanced, but it does have to include owners from both CHs
      DefaultConsistentHash unionCH = chf.union(updatedMembersCH, rebalancedCH);
      for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
         assertTrue(unionCH.locateOwnersForSegment(l).containsAll(updatedMembersCH.locateOwnersForSegment(l)));
         assertTrue(unionCH.locateOwnersForSegment(l).containsAll(rebalancedCH.locateOwnersForSegment(l)));
      }

      // switch to the new CH in the next iteration
      assertEquals(rebalancedCH.getNumSegments(), baseCH.getNumSegments());
      assertEquals(rebalancedCH.getNumOwners(), baseCH.getNumOwners());
      assertEquals(rebalancedCH.getMembers(), newMembers);
      baseCH = rebalancedCH;
      return baseCH;
   }

   private void checkDistribution(ConsistentHash ch, Map<Address, Float> lfMap, boolean allowExtraOwners) {
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getMembers();
      int numNodes = nodes.size();
      int actualNumOwners = computeActualNumOwners(ch.getNumOwners(), nodes, lfMap);

      OwnershipStatistics stats = new OwnershipStatistics(nodes);
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         if (!allowExtraOwners) {
            assertEquals(owners.size(), actualNumOwners);
         } else {
            assertTrue(owners.size() >= actualNumOwners);
         }
         stats.incPrimaryOwned(owners.get(0));
         for (int j = 0; j < owners.size(); j++) {
            Address owner = owners.get(j);
            stats.incOwned(owner);
            assertEquals(owners.indexOf(owner), j, "Found the same owner twice in the owners list");
         }
      }

      float totalCapacity = computeTotalCapacity(nodes, lfMap);
      float maxCapacityFactor = computeMaxCapacityFactor(nodes, lfMap);
      Map<Address, Float> expectedOwnedMap = computeExpectedOwned(numSegments, numNodes, actualNumOwners, nodes, lfMap);
      for (Address node : nodes) {
         float capacityFactor = lfMap != null ? lfMap.get(node) : 1;
         float expectedPrimaryOwned = expectedPrimaryOwned(numSegments, numNodes, totalCapacity, capacityFactor);
         float deviationPrimaryOwned = allowedDeviationPrimaryOwned(numSegments, numNodes, totalCapacity, maxCapacityFactor);
         int minPrimaryOwned = (int) Math.floor(expectedPrimaryOwned - deviationPrimaryOwned);
         int maxPrimaryOwned = (int) Math.ceil(expectedPrimaryOwned + deviationPrimaryOwned);
         if (!allowExtraOwners) {
            int primaryOwned = stats.getPrimaryOwned(node);
            assertTrue(minPrimaryOwned <= primaryOwned);
            assertTrue(primaryOwned <= maxPrimaryOwned);
         }

         float expectedOwned = expectedOwnedMap.get(node);
         float deviationOwned = allowedDeviationOwned(numSegments, actualNumOwners, numNodes, totalCapacity, maxCapacityFactor);
         int minOwned = (int) Math.floor(expectedOwned - deviationOwned);
         int maxOwned = (int) Math.ceil(expectedOwned + deviationOwned);
         int owned = stats.getOwned(node);
         assertTrue(Math.floor(minOwned) <= owned);
         if (!allowExtraOwners) {
            assertTrue(owned <= Math.ceil(maxOwned));
         }
      }
   }

   public int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return Math.min(numOwners, members.size());

      int nodesWithLoad = 0;
      for (Address node : members) {
         if (capacityFactors.get(node) != 0) {
            nodesWithLoad++;
         }
      }
      return Math.min(numOwners, nodesWithLoad);
   }

   protected float expectedPrimaryOwned(int numSegments, int numNodes, float totalCapacity, float nodeLoad) {
      return Math.min(numSegments * nodeLoad / totalCapacity, numSegments);
   }

   protected float allowedDeviationPrimaryOwned(int numSegments, int numNodes, float totalCapacity, float maxCapacityFactor) {
      return numNodes * maxCapacityFactor / totalCapacity;
   }

   protected Map<Address, Float> computeExpectedOwned(int numSegments, int numNodes, int actualNumOwners,
                                                       Collection<Address> nodes,
                                                       final Map<Address, Float> capacityFactors) {
      Map<Address, Float> expectedOwned = new HashMap<Address, Float>();
      if (capacityFactors == null) {
         float expected = Math.min(numSegments, (float) numSegments * actualNumOwners / numNodes);
         for (Address node : nodes) {
            expectedOwned.put(node, expected);
         }
         return expectedOwned;
      }

      List<Address> sortedNodes = new ArrayList<Address>(nodes);
      Collections.sort(sortedNodes, new Comparator<Address>() {
         @Override
         public int compare(Address o1, Address o2) {
            // Reverse order
            return (int) Math.signum(capacityFactors.get(o2) - capacityFactors.get(o1));
         }
      });

      float totalCapacity = computeTotalCapacity(nodes, capacityFactors);

      int remainingCopies = actualNumOwners * numSegments;
      for (Address node : sortedNodes) {
         float nodeLoad = capacityFactors.get(node);
         float nodeSegments;
         if (remainingCopies * nodeLoad / totalCapacity > numSegments) {
            nodeSegments = numSegments;
            totalCapacity -= nodeLoad;
            remainingCopies -= nodeSegments;
         } else {
            nodeSegments = nodeLoad != 0 ? remainingCopies * nodeLoad / totalCapacity : 0;
         }
         expectedOwned.put(node, nodeSegments);
      }
      return expectedOwned;
   }

   protected float allowedDeviationOwned(int numSegments, int actualNumOwners, int numNodes, float totalCapacity,
                                          float maxCapacityFactor) {
      return numNodes * maxCapacityFactor / totalCapacity;
   }

   private float computeTotalCapacity(Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return nodes.size();

      float totalCapacity = 0;
      for (Address node : nodes) {
         totalCapacity += capacityFactors.get(node);
      }
      return totalCapacity;
   }

   private float computeMaxCapacityFactor(Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return 1;

      float maxCapacityFactor = 0;
      for (Address node : nodes) {
         Float capacityFactor = capacityFactors.get(node);
         if (capacityFactor > maxCapacityFactor) {
            maxCapacityFactor = capacityFactor;
         }
      }
      return maxCapacityFactor;
   }

   protected int allowedExtraMoves(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                   int leaverSegments) {
      return (int) Math.ceil(0.25 * oldCH.getNumSegments());
   }

   private void checkMovedSegments(DefaultConsistentHash oldCH, DefaultConsistentHash newCH) {
      int numSegments = oldCH.getNumSegments();
      int numOwners = oldCH.getNumOwners();
      Set<Address> oldMembers = new HashSet<Address>(oldCH.getMembers());
      Set<Address> newMembers = new HashSet<Address>(newCH.getMembers());

      // Compute the number of segments owned by members that left
      int leaverSegments = 0;
      for (Address node : oldMembers) {
         if (!newMembers.contains(node)) {
            leaverSegments += oldCH.getSegmentsForOwner(node).size();
         }
      }

      // Compute the number of segments where an old node became an owner
      int oldMembersAddedSegments = 0;
      for (int segment = 0; segment < numSegments; segment++) {
         ArrayList<Address> oldMembersAdded = new ArrayList<Address>(newCH.locateOwnersForSegment(segment));
         oldMembersAdded.removeAll(oldCH.locateOwnersForSegment(segment));
         oldMembersAdded.retainAll(oldMembers);
         oldMembersAddedSegments += oldMembersAdded.size();
      }

      int movedSegments = oldMembersAddedSegments - leaverSegments;
      int expectedExtraMoves = allowedExtraMoves(oldCH, newCH, leaverSegments);
      if (movedSegments > expectedExtraMoves) {
         log.debugf("%d of %d segments moved, %d (%fx) more than expected (%d)", movedSegments, numSegments,
               movedSegments - expectedExtraMoves, (float) movedSegments / expectedExtraMoves, expectedExtraMoves);
      }
      assert movedSegments <= expectedExtraMoves
               : String.format("Two many moved segments between %s and %s: expected %d, got %d",
            oldCH, newCH, expectedExtraMoves, oldMembersAddedSegments);
   }

   protected <T> Set<T> symmetricalDiff(Collection <T> set1, Collection<T> set2) {
      HashSet<T> commonMembers = new HashSet<T>(set1);
      commonMembers.retainAll(set2);
      HashSet<T> symDiffMembers = new HashSet<T>(set1);
      symDiffMembers.addAll(set2);
      symDiffMembers.removeAll(commonMembers);
      return symDiffMembers;
   }

   public void test1() {
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      TestAddress A = new TestAddress(0, "A");
      TestAddress B = new TestAddress(1, "B");
      TestAddress C = new TestAddress(2, "C");
      TestAddress D = new TestAddress(3, "D");

      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, 60, Arrays.<Address>asList(A), null);
      //System.out.println(ch1);

      DefaultConsistentHash ch2 = chf.updateMembers(ch1, Arrays.<Address>asList(A, B), null);
      ch2 = chf.rebalance(ch2);
      //System.out.println(ch2);

      DefaultConsistentHash ch3 = chf.updateMembers(ch2, Arrays.<Address>asList(A, B, C), null);
      ch3 = chf.rebalance(ch3);
      //System.out.println(ch3);

      DefaultConsistentHash ch4 = chf.updateMembers(ch3, Arrays.<Address>asList(A, B, C, D), null);
      ch4 = chf.rebalance(ch4);
      //System.out.println(ch4);
   }
}
