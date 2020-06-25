package org.infinispan.distribution.ch.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link DefaultConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.DefaultConsistentHashFactoryTest")
public class DefaultConsistentHashFactoryTest extends AbstractInfinispanTest {

   private int iterationCount = 0;

   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new DefaultConsistentHashFactory();
   }

   public void testConsistentHashDistribution() {
      int[] numSegments = {1, 2, 4, 8, 16, 50, 100, 500};
      int[] numNodes = {1, 2, 3, 4, 5, 7, 10, 100};
      int[] numOwners = {1, 2, 3, 5};
      // Since the number of nodes changes, the capacity factors are repeated
      float[][] capacityFactors = {null, {1}, {2}, {1, 100}, {2, 0, 1}};

      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();

      for (int nn : numNodes) {
         List<Address> nodes = new ArrayList<>(nn);
         for (int j = 0; j < nn; j++) {
            nodes.add(new TestAddress(j, "TA"));
         }

         for (int ns : numSegments) {
            if (nn < ns) {
               for (int no : numOwners) {
                  for (float[] lf : capacityFactors) {
                     Map<Address, Float> lfMap = null;
                     if (lf != null) {
                        lfMap = new HashMap<>();
                        for (int i = 0; i < nn; i++) {
                           lfMap.put(nodes.get(i), lf[i % lf.length]);
                        }
                     }
                     testConsistentHashModifications(chf, nodes, ns, no, lfMap);
                  }
               }
            }
         }
      }
   }

   private void testConsistentHashModifications(ConsistentHashFactory<DefaultConsistentHash> chf,
                                                List<Address> nodes, int ns, int no, Map<Address, Float> lfMap) {
      log.tracef("Creating consistent hash with ns=%d, no=%d, members=(%d)%s",
                 ns, no, nodes.size(), membersString(nodes, lfMap));
      DefaultConsistentHash baseCH = chf.create(no, ns, nodes, lfMap);
      assertEquals(lfMap, baseCH.getCapacityFactors());
      checkDistribution(baseCH, lfMap);

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

         List<Address> newMembers = new ArrayList<>(baseMembers);
         HashMap<Address, Float> newCapacityFactors = lfMap != null ? new HashMap<>(lfMap) : null;
         for (int k = 0; k < nodesToRemove; k++) {
            int indexToRemove = Math.abs(MurmurHash3.getInstance().hash(k) % newMembers.size());
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

         log.tracef("Testing consistent hash modifications iteration %d, members=(%d)%s",
                    iterationCount, newMembers.size(), membersString(newMembers, newCapacityFactors));
         baseCH = checkModificationsIteration(chf, baseCH, nodesToAdd, nodesToRemove, newMembers, newCapacityFactors);

         iterationCount++;
      }
   }

   private String membersString(List<Address> newMembers, Map<Address, Float> newCapacityFactors) {
      return newMembers.stream()
                       .map(a -> String.format("%s * %.1f", a, getCapacityFactor(newCapacityFactors, a)))
                       .collect(Collectors.joining(", ", "[", "]"));
   }

   private float getCapacityFactor(Map<Address, Float> capacityFactors, Address a) {
      return capacityFactors != null ? capacityFactors.get(a) : 1f;
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
      long startNanos = System.nanoTime();
      DefaultConsistentHash rebalancedCH = chf.rebalance(updatedMembersCH);
      long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      if (durationMillis > 1) {
         log.tracef("Rebalance took %dms", durationMillis);
      }
      checkDistribution(rebalancedCH, lfMap);

      for (int l = 0; l < rebalancedCH.getNumSegments(); l++) {
         assertTrue(rebalancedCH.locateOwnersForSegment(l).size() >= actualNumOwners);
      }

      checkMovedSegments(baseCH, rebalancedCH, nodesToAdd, nodesToRemove);

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

   protected void checkDistribution(DefaultConsistentHash ch, Map<Address, Float> lfMap) {
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getMembers();
      int numNodesWithLoad = nodesWithLoad(nodes, lfMap);
      int actualNumOwners = computeActualNumOwners(ch.getNumOwners(), nodes, lfMap);

      OwnershipStatistics stats = new OwnershipStatistics(ch, nodes);
      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         assertEquals(owners.size(), actualNumOwners);
         for (int i = 1; i < owners.size(); i++) {
            Address owner = owners.get(i);
            assertEquals(owners.indexOf(owner), i, "Found the same owner twice in the owners list");
         }
      }

      float totalCapacity = computeTotalCapacity(nodes, lfMap);
      Map<Address, Float> expectedOwnedMap =
            computeExpectedOwned(numSegments, numNodesWithLoad, actualNumOwners, nodes, lfMap);
      for (Address node : nodes) {
         float capacityFactor = getCapacityFactor(lfMap, node);
         float expectedPrimaryOwned = expectedPrimaryOwned(numSegments, numNodesWithLoad, totalCapacity, capacityFactor);
         int minPrimaryOwned = (int) Math.floor(minOwned(numSegments, 1, numNodesWithLoad, expectedPrimaryOwned));
         int maxPrimaryOwned = (int) Math.ceil(maxOwned(numSegments, 1, numNodesWithLoad, expectedPrimaryOwned));
         int primaryOwned = stats.getPrimaryOwned(node);
         assertTrue(minPrimaryOwned <= primaryOwned && primaryOwned <= maxPrimaryOwned,
                    String.format("Primary owned (%d) should have been between %d and %d",
                                  primaryOwned, minPrimaryOwned, maxPrimaryOwned));

         float expectedOwned = expectedOwnedMap.get(node);
         int minOwned = (int) Math.floor(minOwned(numSegments, actualNumOwners, numNodesWithLoad, expectedOwned));
         int maxOwned = (int) Math.ceil(maxOwned(numSegments, actualNumOwners, numNodesWithLoad, expectedOwned));
         int owned = stats.getOwned(node);
         assertTrue(minOwned <= owned && owned <= maxOwned,
                    String.format("Owned (%d) should have been between %d and %d", owned, minOwned, maxOwned));
      }
   }

   public int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
      int nodesWithLoad = nodesWithLoad(members, capacityFactors);
      return Math.min(numOwners, nodesWithLoad);
   }

   int nodesWithLoad(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return members.size();

      int nodesWithLoad = 0;
      for (Address node : members) {
         if (capacityFactors.get(node) != 0) {
            nodesWithLoad++;
         }
      }
      return nodesWithLoad;
   }

   protected float expectedPrimaryOwned(int numSegments, int numNodes, float totalCapacity, float nodeLoad) {
      return numSegments * nodeLoad / totalCapacity;
   }

   protected Map<Address, Float> computeExpectedOwned(int numSegments, int numNodes, int actualNumOwners,
                                                      Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      // Insert all nodes in the initial order, even if we're going to replace the values later
      Map<Address, Float> expectedOwned = new LinkedHashMap<>(numNodes * 2);
      float expected = Math.min(numSegments, (float) numSegments * actualNumOwners / numNodes);
      for (Address node : nodes) {
         expectedOwned.put(node, expected);
      }
      if (capacityFactors == null)
         return expectedOwned;

      List<Address> sortedNodes = new ArrayList<>(nodes);
      sortedNodes.sort((o1, o2) -> {
         // Reverse order
         return Float.compare(capacityFactors.get(o2), capacityFactors.get(o1));
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

   protected float maxOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      return expectedOwned + Math.min(.10f * expectedOwned, numNodes - 1);
   }

   protected float minOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      return expectedOwned - Math.max(1, (numSegments * actualNumOwners) / expectedOwned * numNodes);
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

   protected float allowedExtraMoves(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                     int joinerSegments, int leaverSegments) {
      return Math.max(1, 0.05f * oldCH.getNumOwners() * oldCH.getNumSegments());
   }

   private void checkMovedSegments(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                   int nodesAdded, int nodesRemoved) {
      int numSegments = oldCH.getNumSegments();
      int numOwners = oldCH.getNumOwners();
      Set<Address> oldMembers = new HashSet<>(oldCH.getMembers());
      Set<Address> newMembers = new HashSet<>(newCH.getMembers());

      // Compute the number of segments owned by members that left
      int leaverSegments = 0;
      for (Address node : oldMembers) {
         if (!newMembers.contains(node)) {
            leaverSegments += oldCH.getSegmentsForOwner(node).size();
         }
      }
      int joinerSegments = 0;
      for (Address node : newMembers) {
         if (!oldMembers.contains(node)) {
            joinerSegments += newCH.getSegmentsForOwner(node).size();
         }
      }

      // Compute the number of segments where an old node became an owner
      int oldMembersAddedSegments = 0;
      for (int segment = 0; segment < numSegments; segment++) {
         ArrayList<Address> oldMembersAdded = new ArrayList<>(newCH.locateOwnersForSegment(segment));
         oldMembersAdded.removeAll(oldCH.locateOwnersForSegment(segment));
         oldMembersAdded.retainAll(oldMembers);
         oldMembersAddedSegments += oldMembersAdded.size();
      }

      int movedSegments = oldMembersAddedSegments - leaverSegments;
      int expectedExtraMoves = (int) Math.ceil(allowedExtraMoves(oldCH, newCH, joinerSegments, leaverSegments));
      if (movedSegments > expectedExtraMoves / 2) {
         log.tracef("%d of %d*%d extra segments moved, %fx of allowed (%d), %d leavers had %d, %d joiners have %d",
                    movedSegments, numOwners, numSegments, (float) movedSegments / expectedExtraMoves,
                    expectedExtraMoves, nodesRemoved, leaverSegments, nodesAdded, joinerSegments);
      }
      assert movedSegments <= expectedExtraMoves
            : String.format("Two many moved segments between %s and %s: expected %d, got %d",
                            oldCH, newCH, expectedExtraMoves, oldMembersAddedSegments);
   }

   public void test1() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      TestAddress A = new TestAddress(0, "A");
      TestAddress B = new TestAddress(1, "B");
      TestAddress C = new TestAddress(2, "C");
      TestAddress D = new TestAddress(3, "D");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 1f);
      cf.put(C, 1f);
      cf.put(D, 100f);

      DefaultConsistentHash ch1 = chf.create(2, 60, Arrays.asList(A), cf);
      checkDistribution(ch1, cf);

      DefaultConsistentHash ch2 = chf.updateMembers(ch1, Arrays.asList(A, B), cf);
      ch2 = chf.rebalance(ch2);
      checkDistribution(ch2, cf);

      DefaultConsistentHash ch3 = chf.updateMembers(ch2, Arrays.asList(A, B, C), cf);
      ch3 = chf.rebalance(ch3);
      checkDistribution(ch3, cf);

      DefaultConsistentHash ch4 = chf.updateMembers(ch3, Arrays.asList(A, B, C, D), cf);
      ch4 = chf.rebalance(ch4);
      checkDistribution(ch4, cf);
   }
}
