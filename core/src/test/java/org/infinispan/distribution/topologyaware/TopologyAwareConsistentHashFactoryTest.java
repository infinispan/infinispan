/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.topologyaware;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestTopologyAwareAddress;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.TopologyAwareConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.2
 */
@Test(groups = "unit", testName = "topologyaware.TopologyAwareConsistentHashFactoryTest")
public class TopologyAwareConsistentHashFactoryTest extends AbstractInfinispanTest {
   
   private static final Log log = LogFactory.getLog(TopologyAwareConsistentHashFactoryTest.class);
   private static final int CLUSTER_SIZE = 10;
   // Use a high number of segments to avoid collisions
   public static final int NUM_SEGMENTS = 1000;

   private TestTopologyAwareAddress[] testAddresses;
   private List<Address> chMembers;
   private ConsistentHashFactory<DefaultConsistentHash> chf;
   private DefaultConsistentHash ch;

   @BeforeMethod()
   public void setUp() {
      chf = createConsistentHashFactory();
      chMembers = new ArrayList<Address>(CLUSTER_SIZE);
      testAddresses = new TestTopologyAwareAddress[CLUSTER_SIZE];
      for (int i = 0; i < 10; i++) {
         testAddresses[i] = new TestTopologyAwareAddress(i * 100);
         testAddresses[i].setName("a" + i);
      }
   }

   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new TopologyAwareConsistentHashFactory();
   }

   public void testNumberOfOwners() {
      addNode(testAddresses[0], "m0", null, null);

      updateConsistentHash(1);
      assertEquals(ch.locateOwners(testAddresses[0]).size(), 1);
      updateConsistentHash(2);
      assertEquals(ch.locateOwners(testAddresses[0]).size(), 1);

      addNode(testAddresses[1], "m1", null, null);

      updateConsistentHash(1);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 1);
      }
      updateConsistentHash(2);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }
      updateConsistentHash(3);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }

      addNode(testAddresses[2], "m0", null, null);

      updateConsistentHash(1);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 1);
      }
      updateConsistentHash(2);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }
      updateConsistentHash(3);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 3);
      }
      updateConsistentHash(4);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 3);
      }
   }

   public void testDifferentMachines() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m1", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m1", null, null);

      updateConsistentHash(1);
      assertAllLocations(4, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(4, 2, 2, 1, 1);

      updateConsistentHash(3);
      assertAllLocations(4, 3, 2, 1, 1);
   }

   public void testNumOwnerBiggerThanAvailableNodes() {
      // test first with one node
      addNode(testAddresses[0], "m0", null, null);

      updateConsistentHash(2);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(99);
      assertAllLocations(6, 1, 1, 1, 1);

      // test with two nodes
      addNode(testAddresses[1], "m0", null, null);

      updateConsistentHash(3);
      assertAllLocations(6, 2, 1, 1, 1);
      updateConsistentHash(4);
      assertLocation(testAddresses[1], 2, 1, 1, 1);

      // test with three nodes
      addNode(testAddresses[2], "m0", null, null);

      updateConsistentHash(4);
      assertAllLocations(6, 3, 1, 1, 1);
      updateConsistentHash(6);
      assertLocation(testAddresses[1], 3, 1, 1, 1);
      assertLocation(testAddresses[2], 3, 1, 1, 1);
   }
   
   public void testDifferentMachines2() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m1", null, null);
      addNode(testAddresses[3], "m1", null, null);
      addNode(testAddresses[4], "m2", null, null);
      addNode(testAddresses[5], "m2", null, null);

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 1, 1);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 1, 1);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 3, 1, 1);
   }

   public void testDifferentRacksAndMachines() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m2", "r1", null);
      addNode(testAddresses[5], "m2", "r2", null);

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 2, 1);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 3, 1);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 4, 3, 1);
   }

   public void testAllSameMachine() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m0", null, null);
      addNode(testAddresses[4], "m0", null, null);
      addNode(testAddresses[5], "m0", null, null);

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 1, 1, 1);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 1, 1, 1);
   }

   public void testDifferentSites() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s0");
      addNode(testAddresses[2], "m2", null, "s1");
      addNode(testAddresses[3], "m3", null, "s1");

      updateConsistentHash(1);
      assertAllLocations(4, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(4, 2, 2, 2, 2);

      updateConsistentHash(3);
      assertAllLocations(4, 3, 3, 2, 2);
   }

   public void testSitesMachines2() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s1");
      addNode(testAddresses[2], "m2", null, "s0");
      addNode(testAddresses[3], "m3", null, "s2");
      addNode(testAddresses[4], "m4", null, "s1");
      addNode(testAddresses[5], "m5", null, "s1");

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 2, 2);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 3, 3);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 4, 3, 3);
   }

   public void testSitesMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", null, "r0");
      addNode(testAddresses[1], "m0", null, "r1");
      addNode(testAddresses[2], "m0", null, "r0");
      addNode(testAddresses[3], "m0", null, "r2");
      addNode(testAddresses[4], "m0", null, "r1");
      addNode(testAddresses[5], "m0", null, "r1");

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 2, 2);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 3, 3);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 3, 3, 3);
   }

   public void testDifferentRacks() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r1", null);

      updateConsistentHash(1);
      assertAllLocations(4, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(4, 2, 2, 2, 1);

      updateConsistentHash(3);
      assertAllLocations(4, 3, 3, 2, 1);
   }

   public void testRacksMachines2() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r1", null);
      addNode(testAddresses[2], "m2", "r0", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m4", "r1", null);
      addNode(testAddresses[5], "m5", "r1", null);

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 2, 1);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 3, 1);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 4, 3, 1);
   }

   public void testRacksMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m0", "r1", null);
      addNode(testAddresses[2], "m0", "r0", null);
      addNode(testAddresses[3], "m0", "r2", null);
      addNode(testAddresses[4], "m0", "r1", null);
      addNode(testAddresses[5], "m0", "r1", null);

      updateConsistentHash(1);
      assertAllLocations(6, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(6, 2, 2, 2, 1);

      updateConsistentHash(3);
      assertAllLocations(6, 3, 3, 3, 1);

      updateConsistentHash(4);
      assertAllLocations(6, 4, 3, 3, 1);
   }

   public void testComplexScenario() {
      // {s0: {r0: {m0, m1}}, s1: {r0: {m0, m1, m2}, r1: {m0}}}
      addNode(testAddresses[0], "m2", "r0", "s1");
      addNode(testAddresses[1], "m1", "r0", "s0");
      addNode(testAddresses[2], "m1", "r0", "s1");
      addNode(testAddresses[3], "m1", "r1", "s0");
      addNode(testAddresses[4], "m0", "r0", "s1");
      addNode(testAddresses[5], "m0", "r1", "s1");
      addNode(testAddresses[6], "m0", "r1", "s0");
      addNode(testAddresses[7], "m0", "r0", "s1");
      addNode(testAddresses[8], "m0", "r0", "s0");
      addNode(testAddresses[9], "m0", "r0", "s0");

      updateConsistentHash(1);
      assertAllLocations(10, 1, 1, 1, 1);

      updateConsistentHash(2);
      assertAllLocations(10, 2, 2, 2, 2);

      updateConsistentHash(3);
      assertAllLocations(10, 3, 3, 3, 2);

      updateConsistentHash(4);
      assertAllLocations(10, 4, 4, 4, 2);
   }

   private void assertAllLocations(int addressCount, int expectedOwners, int expectedMachines,
                                   int expectedRacks, int expectedSites) {
      for (int i = 0; i < addressCount; i++) {
         assertLocation(testAddresses[i], expectedOwners, expectedMachines, expectedRacks, expectedSites);
      }
   }

   public void testConsistencyWhenNodeLeaves() {
      addNode(testAddresses[0], "m2", "r0", "s1");
      addNode(testAddresses[1], "m1", "r0", "s0");
      addNode(testAddresses[2], "m1", "r0", "s1");
      addNode(testAddresses[3], "m1", "r1", "s0");
      addNode(testAddresses[4], "m0", "r0", "s1");
      addNode(testAddresses[5], "m0", "r1", "s1");
      addNode(testAddresses[6], "m0", "r1", "s0");
      addNode(testAddresses[7], "m0", "r0", "s3");
      addNode(testAddresses[8], "m0", "r0", "s2");
      addNode(testAddresses[9], "m0", "r0", "s0");

      updateConsistentHash(3);
      List<List<Address>> owners = new ArrayList<List<Address>>();
      for (int i = 0; i < 10; i++) {
         owners.add(ch.locateOwners(testAddresses[i]));
      }

      for (Address addr: chMembers) {
         System.out.println("addr = " + addr);
         List<Address> addressCopy = new ArrayList<Address>(chMembers);
         addressCopy.remove(addr);
         DefaultConsistentHash newCH = chf.updateMembers(ch, addressCopy);
         newCH = chf.rebalance(newCH);

         for (int i = 0; i < 10; i++) {
            checkConsistency(testAddresses[i], 3, owners.get(i), addr, newCH);
         }
      }
   }

   private void checkConsistency(Address key, int replCount, List<Address> originalOwners,
                                 Address removedAddress, DefaultConsistentHash newCH) {
      if (key.equals(removedAddress))
         return;

      List<Address> currentOwners = newCH.locateOwners(key);
      originalOwners = new ArrayList<Address>(originalOwners);
      originalOwners.remove(removedAddress);

      assertEquals(replCount, currentOwners.size(), currentOwners.toString());
      assert currentOwners.containsAll(originalOwners)
            : "Current backups are: " + currentOwners + "Previous: " + originalOwners;
   }


   private void assertLocation(Object key, int expectedOwners, int expectedMachines, int expectedRacks,
                               int expectedSites) {
      List<Address> received = ch.locateOwners(key);

      // Check the number of addresses and uniqueness
      assertEquals(received.size(), expectedOwners);
      Set<Address> receivedUnique = new HashSet<Address>(received);
      assertEquals(receivedUnique.size(), expectedOwners);

      // Check the number of machines
      Set<String> receivedMachines = new HashSet<String>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedMachines.add(taa.getMachineId() + "|" + taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedMachines.size(), expectedMachines);

      // Check the number of racks
      Set<String> receivedRacks = new HashSet<String>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedRacks.add(taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedRacks.size(), expectedRacks);

      // Check the number of sites
      Set<String> receivedSites = new HashSet<String>();
      for (Address a : received) {
         receivedSites.add(((TopologyAwareAddress) a).getSiteId());
      }
      assertEquals(receivedSites.size(), expectedSites);

      for (Address testAddress : testAddresses) {
         boolean shouldBeLocal = received.contains(testAddress);
         boolean isLocal = ch.isKeyLocalToNode(testAddress, key);
         assertEquals(isLocal, shouldBeLocal);
      }
   }

   private void addNode(TestTopologyAwareAddress address,
                        String machineId, String rackId, String siteId) {
      address.setSiteId(siteId);
      address.setRackId(rackId);
      address.setMachineId(machineId);
      chMembers.add(address);
   }

   private void updateConsistentHash(int numOwners) {
      ch = chf.create(new MurmurHash3(), numOwners, NUM_SEGMENTS, chMembers);
   }
}
