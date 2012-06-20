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
package org.infinispan.distribution.newch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "unit", testName = "distribution.NewDefaultConsistentHashFactoryTest")
public class NewDefaultConsistentHashFactoryTest extends AbstractInfinispanTest {

   private static int iterationCount = 0;

   public void testConsistentHashDistribution() {
      int[] numSegments = {1, 2, 4, 8, 16, 64, 128, 256, 512, 1024};
      int[] numNodes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000};
      int[] numOwners = {1, 2, 3, 5, 10};

      NewDefaultConsistentHashFactory chf = new NewDefaultConsistentHashFactory();
      Hash hashFunction = new MurmurHash3();

      for (int i = 0; i < numNodes.length; i++) {
         int nn = numNodes[i];
         List<Address> nodes = new ArrayList<Address>(nn);
         for (int j = 0; j < nn; j++) {
            nodes.add(new TestAddress(j));
         }

         for (int j = 0; j < numSegments.length; j++) {
            int ns = numSegments[j];
            if (nn < ns) {
               for (int k = 0; k < numOwners.length; k++) {
                  int no = numOwners[k];
                  NewDefaultConsistentHash ch = (NewDefaultConsistentHash) chf.createConsistentHash(hashFunction, no, ns, nodes);
                  checkDistribution(ch);

                  testConsistentHashModifications(chf, ch);
               }
            }
         }
      }
   }

   private void checkDistribution(NewAdvancedConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getNodes();
      int numNodes = nodes.size();
      int actualNumOwners = Math.min(ch.getNumOwners(), numNodes);

      CHStatistics stats = new CHStatistics(nodes);
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         Assert.assertEquals(owners.size(), actualNumOwners);
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
         int primaryOwned = stats.getPrimaryOwned(node);
         assertTrue(minPrimaryOwned <= primaryOwned);
         assertTrue(primaryOwned <= maxPrimaryOwned);

         int owned = stats.getOwned(node);
         assertTrue(minOwned <= owned);
         assertTrue(owned <= maxOwned);
      }
   }

   private void testConsistentHashModifications(NewDefaultConsistentHashFactory chf,
                                                NewDefaultConsistentHash baseCH) {
      // each element in the array is a pair of numbers: the first is the number of nodes to add
      // the second is the number of nodes to remove (the index of the removed nodes are pseudo-random)
      int[][] nodeChanges = {{0, 0}, {1, 0}, {2, 0}, {0, 1}, {0, 2}, {1, 1}, {2, 2}, {10, 0}, {0, 10}, {10, 10}};

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();
      // starting point, so that we don't confuse nodes
      int nodeIndex = baseCH.getNodes().size();
      for (int i = 0; i < nodeChanges.length; i++) {
         int nodesToAdd = nodeChanges[i][0];
         int nodesToRemove = nodeChanges[i][1];
         if (nodesToRemove > baseCH.getNodes().size())
            break;

         List<Address> newNodes = new ArrayList<Address>(baseCH.getNodes());
         for (int k = 0; k < nodesToRemove; k++) {
            newNodes.remove(Math.abs(baseCH.getHashFunction().hash(k) % newNodes.size()));
         }
         for (int k = 0; k < nodesToAdd; k++) {
            newNodes.add(new TestAddress(nodeIndex++));
         }
         if (newNodes.size() > baseCH.getNumSegments())
            break;

         log.debugf("Testing consistent hash modifications iteration %d. Initial CH is %s. New members are %s",
               iterationCount, baseCH, newNodes);

         if (nodesToAdd == 0 && nodesToRemove == 0) {
            assertFalse(chf.needNewConsistentHash(baseCH, newNodes));
            continue;
         }

         // first phase: just remove the leavers
         assertTrue(chf.needNewConsistentHash(baseCH, newNodes));
         NewDefaultConsistentHash newCH = (NewDefaultConsistentHash) chf.createConsistentHash(baseCH,
               newNodes);
         if (nodesToRemove > 0) {
            for (int l = 0; l < newCH.getNumSegments(); l++) {
               assertTrue(newCH.locateOwnersForSegment(l).size() >= 0);
            }

            // second phase: add new owners to each segment
            // if we didn't remove any node, the first phase would have actually been the second phase
            assertTrue(chf.needNewConsistentHash(baseCH, newNodes));
            newCH = (NewDefaultConsistentHash) chf.createConsistentHash(newCH, newNodes);
         }

         for (int l = 0; l < newCH.getNumSegments(); l++) {
            int actualNumOwners = Math.min(newCH.getNodes().size(), newCH.getNumOwners());
            assertTrue(newCH.locateOwnersForSegment(l).size() >= actualNumOwners);
         }

         // third phase: prune extra owners
         newCH = (NewDefaultConsistentHash) chf.createConsistentHash(newCH,
               newNodes);
         checkDistribution(newCH);

         // switch to the new CH in the next iteration
         assertEquals(newCH.getNumSegments(), baseCH.getNumSegments());
         assertEquals(newCH.getNumOwners(), baseCH.getNumOwners());
         baseCH = newCH;
         iterationCount++;
      }
   }
}

