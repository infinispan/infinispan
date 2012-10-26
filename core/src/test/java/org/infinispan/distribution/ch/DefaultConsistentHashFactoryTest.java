/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link DefaultConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "ch.DefaultConsistentHashFactoryTest")
public class DefaultConsistentHashFactoryTest extends AbstractInfinispanTest {

   private int iterationCount = 0;

   protected ConsistentHashFactory createConsistentHashFactory() {
      return new DefaultConsistentHashFactory();
   }

   public void testConsistentHashDistribution() {
      int[] numSegments = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};
      int[] numNodes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000};
      int[] numOwners = {1, 2, 3, 5};

      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Hash hashFunction = new MurmurHash3();

      for (int nn : numNodes) {
         List<Address> nodes = new ArrayList<Address>(nn);
         for (int j = 0; j < nn; j++) {
            nodes.add(new TestAddress(j));
         }

         for (int ns : numSegments) {
            if (nn < ns) {
               for (int no : numOwners) {
                  DefaultConsistentHash ch = chf.create(hashFunction, no, ns, nodes);
                  checkDistribution(ch, false);

                  testConsistentHashModifications(chf, ch);
               }
            }
         }
      }
   }

   private void testConsistentHashModifications(ConsistentHashFactory<DefaultConsistentHash> chf, DefaultConsistentHash baseCH) {
      // each element in the array is a pair of numbers: the first is the number of nodes to add
      // the second is the number of nodes to remove (the index of the removed nodes are pseudo-random)
      int[][] nodeChanges = {{1, 0}, {2, 0}, {0, 1}, {0, 2}, {1, 1}, {1, 2}, {2, 1}, {10, 0}, {0, 10}};

      // check that the base CH is already balanced
      assertSame(baseCH, chf.updateMembers(baseCH, baseCH.getMembers()));
      assertSame(baseCH, chf.rebalance(baseCH));

      // starting point, so that we don't confuse nodes
      int nodeIndex = baseCH.getMembers().size();
      for (int i = 0; i < nodeChanges.length; i++) {
         int nodesToAdd = nodeChanges[i][0];
         int nodesToRemove = nodeChanges[i][1];
         if (nodesToRemove > baseCH.getMembers().size())
            break;

         List<Address> newMembers = new ArrayList<Address>(baseCH.getMembers());
         for (int k = 0; k < nodesToRemove; k++) {
            newMembers.remove(Math.abs(baseCH.getHashFunction().hash(k) % newMembers.size()));
         }
         for (int k = 0; k < nodesToAdd; k++) {
            newMembers.add(new TestAddress(nodeIndex++));
         }

         log.tracef("Testing consistent hash modifications iteration %d. Initial CH is %s. New members are %s",
               iterationCount, baseCH, newMembers);
         baseCH = checkModificationsIteration(chf, baseCH, nodesToAdd, nodesToRemove, newMembers);


         iterationCount++;
      }
   }

   private DefaultConsistentHash checkModificationsIteration(ConsistentHashFactory<DefaultConsistentHash> chf,
                                                             DefaultConsistentHash baseCH, int nodesToAdd,
                                                             int nodesToRemove, List<Address> newMembers) {
      int actualNumOwners = Math.min(newMembers.size(), baseCH.getNumOwners());

      // first phase: just update the members list, removing the leavers
      // and adding new owners, but not necessarily assigning segments to them
      DefaultConsistentHash updatedMembersCH = chf.updateMembers(baseCH, newMembers);
      if (nodesToRemove > 0) {
         for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
            assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() > 0);
            assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() <= actualNumOwners);
         }
      }

      // second phase: rebalance with the new members list
      DefaultConsistentHash rebalancedCH = chf.rebalance(updatedMembersCH);
      checkDistribution(rebalancedCH, false);

      for (int l = 0; l < rebalancedCH.getNumSegments(); l++) {
         assertTrue(rebalancedCH.locateOwnersForSegment(l).size() >= actualNumOwners);
      }

      checkMovedSegments(baseCH, rebalancedCH, nodesToAdd);

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

   private void checkDistribution(ConsistentHash ch, boolean allowExtraOwners) {
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getMembers();
      int numNodes = nodes.size();
      int actualNumOwners = Math.min(ch.getNumOwners(), numNodes);

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

      int minPrimaryOwned = minPrimaryOwned(numSegments, numNodes);
      int maxPrimaryOwned = maxPrimaryOwned(numSegments, numNodes);
      int minOwned = minOwned(numSegments, numNodes, actualNumOwners);
      int maxOwned = maxOwned(numSegments, numNodes, actualNumOwners);
      for (Address node : nodes) {
         if (!allowExtraOwners) {
            int primaryOwned = stats.getPrimaryOwned(node);
            assertTrue(minPrimaryOwned <= primaryOwned);
            assertTrue(primaryOwned <= maxPrimaryOwned);
         }

         int owned = stats.getOwned(node);
         assertTrue(minOwned <= owned);
         if (!allowExtraOwners) {
            assertTrue(owned <= maxOwned);
         }
      }
   }

   protected int minPrimaryOwned(int numSegments, int numNodes) {
      return numSegments / numNodes;
   }

   protected int maxPrimaryOwned(int numSegments, int numNodes) {
      return (int) Math.ceil((double)numSegments / numNodes);
   }

   protected int minOwned(int numSegments, int numNodes, int actualNumOwners) {
      return numSegments * actualNumOwners / numNodes;
   }

   protected int maxOwned(int numSegments, int numNodes, int actualNumOwners) {
      return (int) Math.ceil((double)numSegments * actualNumOwners / numNodes);
   }

   protected int allowedMoves(int numSegments, int numOwners, Collection<Address> oldMembers,
                                 Collection<Address> newMembers) {
      int minMembers = Math.min(oldMembers.size(), newMembers.size());
      int maxMembers = Math.max(oldMembers.size(), newMembers.size());
      if (maxMembers > numSegments)
         return numSegments * numOwners; // don't do any checks in this case

      Set<Address> addedMembers = new HashSet<Address>(newMembers);
      addedMembers.removeAll(oldMembers);
      Set<Address> removedMembers = new HashSet<Address>(oldMembers);
      removedMembers.removeAll(newMembers);

      // TODO removedMembers should not matter
      int minMoves = (addedMembers.size() + removedMembers.size()) * numSegments * numOwners / minMembers;
      // need to account for changes in the number of assigned segments based on a node's position in the members list
      int extraMoves = maxMembers % numSegments;
      // 1.5 is a "inefficiency factor"
      return (int) (1.5 * (minMoves + extraMoves));
   }

   private void checkMovedSegments(DefaultConsistentHash oldCH, DefaultConsistentHash newCH, int addedNodes) {
      int numSegments = oldCH.getNumSegments();
      int numOwners = oldCH.getNumOwners();
      List<Address> oldMembers = oldCH.getMembers();
      List<Address> newMembers = newCH.getMembers();

      // compute the number of segments that changed owners even though their old owners were still members
      int movedSegments = 0;
      for (int i = 0; i < numSegments; i++) {
         ArrayList<Address> lostOwners = new ArrayList<Address>(oldCH.locateOwnersForSegment(i));
         lostOwners.removeAll(newCH.locateOwnersForSegment(i));
         lostOwners.retainAll(newMembers);
         movedSegments += lostOwners.size();
      }

      int expectedMoves = allowedMoves(numSegments, numOwners, oldMembers, newMembers);
      assert movedSegments <= expectedMoves
            : String.format("Two many moved segments between %s and %s: expected %d, got %d",
                            oldCH, newCH, expectedMoves, movedSegments);
   }

   protected <T> Set<T> symmetricalDiff(Collection<T> set1, Collection<T> set2) {
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

      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, 60, Arrays.<Address>asList(A));
      System.out.println(ch1);

      DefaultConsistentHash ch2 = chf.updateMembers(ch1, Arrays.<Address>asList(A, B));
      ch2 = chf.rebalance(ch2);
      System.out.println(ch2);

      DefaultConsistentHash ch3 = chf.updateMembers(ch2, Arrays.<Address>asList(A, B, C));
      ch3 = chf.rebalance(ch3);
      System.out.println(ch3);

      DefaultConsistentHash ch4 = chf.updateMembers(ch3, Arrays.<Address>asList(A, B, C, D));
      ch4 = chf.rebalance(ch4);
      System.out.println(ch4);
   }
}
