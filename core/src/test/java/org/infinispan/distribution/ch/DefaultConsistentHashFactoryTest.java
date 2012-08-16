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
import java.util.List;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test(groups = "unit", testName = "distribution.DefaultConsistentHashFactoryTest")
public class DefaultConsistentHashFactoryTest extends AbstractInfinispanTest {

   private static int iterationCount = 0;

   public void testConsistentHashDistribution() {
      int[] numSegments = {1, 2, 4, 8, 16, 64, 128, 256, 512, 1024};
      int[] numNodes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000};
      int[] numOwners = {1, 2, 3, 5, 10};

      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
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

      int minPrimaryOwned = numSegments / numNodes;
      int maxPrimaryOwned = (int) Math.ceil((double)numSegments / numNodes);
      int minOwned = numSegments * actualNumOwners / numNodes;
      int maxOwned = (int) Math.ceil((double)numSegments * actualNumOwners / numNodes);
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

   private void testConsistentHashModifications(DefaultConsistentHashFactory chf, DefaultConsistentHash baseCH) {
      // each element in the array is a pair of numbers: the first is the number of nodes to add
      // the second is the number of nodes to remove (the index of the removed nodes are pseudo-random)
      int[][] nodeChanges = {{1, 0}, {2, 0}, {0, 1}, {0, 2}, {1, 1}, {2, 2}, {10, 0}, {0, 10}, {10, 10}};

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

         log.debugf("Testing consistent hash modifications iteration %d. Initial CH is %s. New members are %s",
               iterationCount, baseCH, newMembers);

         // first phase: just update the members list, removing the leavers
         // and adding new owners, but not necessarily assigning segments to them
         DefaultConsistentHash updatedMembersCH = chf.updateMembers(baseCH, newMembers);
         if (nodesToRemove > 0) {
            for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
               assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() > 0);
            }
         }

         // second phase: rebalance with the new members list
         DefaultConsistentHash inclRebalancedCH = chf.rebalance(updatedMembersCH);
         checkDistribution(inclRebalancedCH, false);

         int actualNumOwners = Math.min(inclRebalancedCH.getMembers().size(), inclRebalancedCH.getNumOwners());
         for (int l = 0; l < inclRebalancedCH.getNumSegments(); l++) {
            assertTrue(inclRebalancedCH.locateOwnersForSegment(l).size() >= actualNumOwners);
         }

         // third phase: prune extra owners
         DefaultConsistentHash exclRebalancedCH = chf.rebalance(updatedMembersCH);
         DefaultConsistentHash exclRebalancedCH2 = chf.rebalance(inclRebalancedCH);
         // TODO sometimes the order of the backup owners is not the same because the node may be removed and then re-added at the end of the list
         assertEquals(exclRebalancedCH2, exclRebalancedCH);
         checkDistribution(exclRebalancedCH, false);

         // switch to the new CH in the next iteration
         assertEquals(exclRebalancedCH.getNumSegments(), baseCH.getNumSegments());
         assertEquals(exclRebalancedCH.getNumOwners(), baseCH.getNumOwners());
         assertEquals(exclRebalancedCH.getMembers(), newMembers);
         baseCH = exclRebalancedCH;
         iterationCount++;
      }
   }
}
