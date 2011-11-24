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
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.TestTopologyAwareAddress;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "unit", testName = "topologyaware.TopologyAwareConsistentHashTest")
public class TopologyAwareConsistentHashTest extends AbstractInfinispanTest {
   
   private static final Log log = LogFactory.getLog(TopologyAwareConsistentHashTest.class);

   protected TopologyAwareConsistentHash ch;
   protected HashSet<Address> addresses;
   TestTopologyAwareAddress[] testAddresses;

   @BeforeMethod
   public void setUp() {
      ch = new TopologyAwareConsistentHash(new MurmurHash3());
      addresses = new HashSet<Address>();
      for (int i = 0; i < 10; i++) {
          addresses.add(new TestTopologyAwareAddress(i * 100));
      }
      ch.setCaches(addresses);
      Set<Address> tmp = ch.getCaches();
      int i = 0;
      testAddresses = new TestTopologyAwareAddress[tmp.size()];
      for (Address a: tmp) testAddresses[i++] = (TestTopologyAwareAddress) a;

      ch = new TopologyAwareConsistentHash(new MurmurHash3());
      addresses.clear();
   }

   public void testNumberOfOwners() {
      addNode(testAddresses[0], "m0", null, null);
      setAddresses();

      assert(ch.locate(testAddresses[0], 1).size() == 1);
      assert(ch.locate(testAddresses[0], 2).size() == 1);

      addNode(testAddresses[1], "m1", null, null);
      setAddresses();

      for (int i = 0; i < testAddresses.length; i++) {
         assert(ch.locate(testAddresses[i], 1).size() == 1);
         assert(ch.locate(testAddresses[i], 2).size() == 2);
         assert(ch.locate(testAddresses[i], 3).size() == 2);
      }

      addNode(testAddresses[2], "m0", null, null);
      setAddresses();

      for (int i = 0; i < testAddresses.length; i++) {
         assert(ch.locate(testAddresses[i], 1).size() == 1);
         assert(ch.locate(testAddresses[i], 2).size() == 2);
         assert(ch.locate(testAddresses[i], 3).size() == 3);
         assert(ch.locate(testAddresses[i], 4).size() == 3);
      }
   }

   public void testDifferentMachines() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m1", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m1", null, null);
      setAddresses();

      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[0]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[0], testAddresses[1]);


   }

   public void testNumOwnerBiggerThanAvailableNodes() {
      // test first with one node
      addNode(testAddresses[0], "m0", null, null);
      setAddresses();

      assertLocation(testAddresses[0], 2, true, testAddresses[0]);
      assertLocation(testAddresses[0], 99, true, testAddresses[0]);

      // test with two nodes
      addNode(testAddresses[1], "m0", null, null);
      setAddresses();

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 4, true, testAddresses[1], testAddresses[0]);

      // test with three nodes
      addNode(testAddresses[2], "m0", null, null);
      setAddresses();

      assertLocation(testAddresses[0], 4, true, testAddresses[0], testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[1], 5, true, testAddresses[1], testAddresses[2], testAddresses[0]);
      assertLocation(testAddresses[2], 6, true, testAddresses[2], testAddresses[0], testAddresses[1]);
   }
   
   public void testDifferentMachines2() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m1", null, null);
      addNode(testAddresses[3], "m1", null, null);
      addNode(testAddresses[4], "m2", null, null);
      addNode(testAddresses[5], "m2", null, null);
      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[2]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[4]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[2], testAddresses[4]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[4]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[0], testAddresses[2]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[2]);
   }

   public void testDifferentRacksAndMachines() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m2", "r1", null);
      addNode(testAddresses[5], "m2", "r2", null);
      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[2]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[5]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[0]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[5], testAddresses[0]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[2]);
   }

   public void testAllSameMachine() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m0", null, null);
      addNode(testAddresses[4], "m0", null, null);
      addNode(testAddresses[5], "m0", null, null);

      setAddresses();
      System.out.println("CH is " + ch);

      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[5]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[5]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[5], testAddresses[0]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[1]);
   }

   public void testDifferentSites() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s0");
      addNode(testAddresses[2], "m2", null, "s1");
      addNode(testAddresses[3], "m3", null, "s1");
      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[2]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[0]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[2], testAddresses[1]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[0], testAddresses[1]);
   }

   public void testSitesMachines2() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s1");
      addNode(testAddresses[2], "m2", null, "s0");
      addNode(testAddresses[3], "m3", null, "s2");
      addNode(testAddresses[4], "m4", null, "s1");
      addNode(testAddresses[5], "m5", null, "s1");

      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[3]);
   }

   public void testSitesMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", null, "r0");
      addNode(testAddresses[1], "m0", null, "r1");
      addNode(testAddresses[2], "m0", null, "r0");
      addNode(testAddresses[3], "m0", null, "r2");
      addNode(testAddresses[4], "m0", null, "r1");
      addNode(testAddresses[5], "m0", null, "r1");

      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[3]);
   }

   public void testDifferentRacks() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r1", null);
      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[2]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[0]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[2], testAddresses[1]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[0], testAddresses[1]);
   }

   public void testRacksMachines2() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r1", null);
      addNode(testAddresses[2], "m2", "r0", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m4", "r1", null);
      addNode(testAddresses[5], "m5", "r1", null);

      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[3]);
   }

   public void testRacksMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m0", "r1", null);
      addNode(testAddresses[2], "m0", "r0", null);
      addNode(testAddresses[3], "m0", "r2", null);
      addNode(testAddresses[4], "m0", "r1", null);
      addNode(testAddresses[5], "m0", "r1", null);

      setAddresses();
      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[0]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[0], testAddresses[3]);
   }

   public void testComplexScenario() {
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
      setAddresses();

      assertLocation(testAddresses[0], 1, true, testAddresses[0]);
      assertLocation(testAddresses[1], 1, true, testAddresses[1]);
      assertLocation(testAddresses[2], 1, true, testAddresses[2]);
      assertLocation(testAddresses[3], 1, true, testAddresses[3]);
      assertLocation(testAddresses[4], 1, true, testAddresses[4]);
      assertLocation(testAddresses[5], 1, true, testAddresses[5]);
      assertLocation(testAddresses[6], 1, true, testAddresses[6]);
      assertLocation(testAddresses[7], 1, true, testAddresses[7]);
      assertLocation(testAddresses[8], 1, true, testAddresses[8]);
      assertLocation(testAddresses[9], 1, true, testAddresses[9]);

      assertLocation(testAddresses[0], 2, true, testAddresses[0], testAddresses[1]);
      assertLocation(testAddresses[1], 2, true, testAddresses[1], testAddresses[2]);
      assertLocation(testAddresses[2], 2, true, testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[3], 2, true, testAddresses[3], testAddresses[4]);
      assertLocation(testAddresses[4], 2, true, testAddresses[4], testAddresses[6]);
      assertLocation(testAddresses[5], 2, true, testAddresses[5], testAddresses[6]);
      assertLocation(testAddresses[6], 2, true, testAddresses[6], testAddresses[7]);
      assertLocation(testAddresses[7], 2, true, testAddresses[7], testAddresses[8]);
      assertLocation(testAddresses[8], 2, true, testAddresses[8], testAddresses[0]);
      assertLocation(testAddresses[9], 2, true, testAddresses[9], testAddresses[0]);

      assertLocation(testAddresses[0], 3, true, testAddresses[0], testAddresses[1], testAddresses[3]);
      assertLocation(testAddresses[1], 3, true, testAddresses[1], testAddresses[2], testAddresses[3]);
      assertLocation(testAddresses[2], 3, true, testAddresses[2], testAddresses[3], testAddresses[5]);
      assertLocation(testAddresses[3], 3, true, testAddresses[3], testAddresses[4], testAddresses[5]);
      assertLocation(testAddresses[4], 3, true, testAddresses[4], testAddresses[6], testAddresses[5]);
      assertLocation(testAddresses[5], 3, true, testAddresses[5], testAddresses[6], testAddresses[7]);
      assertLocation(testAddresses[6], 3, true, testAddresses[6], testAddresses[7], testAddresses[8]);
      assertLocation(testAddresses[7], 3, true, testAddresses[7], testAddresses[8], testAddresses[3]);
      assertLocation(testAddresses[8], 3, true, testAddresses[8], testAddresses[0], testAddresses[3]);
      assertLocation(testAddresses[9], 3, true, testAddresses[9], testAddresses[0], testAddresses[3]);
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
      setAddresses();

      List<Address> testAddresses0List = ch.locate(testAddresses[0], 3);
      List<Address> testAddresses1List = ch.locate(testAddresses[1], 3);
      List<Address> testAddresses2List = ch.locate(testAddresses[2], 3);
      List<Address> testAddresses3List = ch.locate(testAddresses[3], 3);
      List<Address> testAddresses4List = ch.locate(testAddresses[4], 3);
      List<Address> testAddresses5List = ch.locate(testAddresses[5], 3);
      List<Address> testAddresses6List = ch.locate(testAddresses[6], 3);
      List<Address> testAddresses7List = ch.locate(testAddresses[7], 3);
      List<Address> testAddresses8List = ch.locate(testAddresses[8], 3);
      List<Address> testAddresses9List = ch.locate(testAddresses[9], 3);

      for (Address addr: addresses) {
         System.out.println("addr = " + addr);
         Set<Address> addressCopy = (Set<Address>) addresses.clone();
         addressCopy.remove(addr);
         ch.setCaches(addressCopy);
         checkConsistency(testAddresses0List, testAddresses[0], addr, 3);
         checkConsistency(testAddresses1List, testAddresses[1], addr, 3);
         checkConsistency(testAddresses2List, testAddresses[2], addr, 3);
         checkConsistency(testAddresses3List, testAddresses[3], addr, 3);
         checkConsistency(testAddresses4List, testAddresses[4], addr, 3);
         checkConsistency(testAddresses5List, testAddresses[5], addr, 3);
         checkConsistency(testAddresses6List, testAddresses[6], addr, 3);
         checkConsistency(testAddresses7List, testAddresses[7], addr, 3);
         checkConsistency(testAddresses8List, testAddresses[8], addr, 3);
         checkConsistency(testAddresses9List, testAddresses[9], addr, 3);
      }
   }

   private void checkConsistency(List<Address> testAddressesList, Address testAddresses, Address addr, int replCount) {
      testAddressesList = new ArrayList(testAddressesList);
      testAddressesList.remove(addr);
      if (testAddresses.equals(addr)) return;
      List<Address> currentBackupList = ch.locate(testAddresses, replCount);
      assertEquals(replCount, currentBackupList.size(), currentBackupList.toString());
      assert currentBackupList.containsAll(testAddressesList) : "Current backups are: " + currentBackupList + "Previous: " + testAddressesList;
   }


   private void assertLocation(Object key, int numOwners, boolean enforceSequence, Address... expected) {
      List<Address> received = ch.locate(key, numOwners);

      if (expected == null) {
         assert received.isEmpty();
      }
      assertEquals(received.size(), expected.length);
      if (enforceSequence) {
         assert received.equals(Arrays.asList(expected)) : "Received: " + received + " Expected: " + Arrays.toString(expected);
      } else {
         assert received.containsAll(Arrays.asList(expected)) : "Received: " + received + " Expected: " + Arrays.toString(expected);
      }

      for (Address testAddress : testAddresses) {
         boolean shouldBeLocal = received.contains(testAddress);
         boolean isLocal = ch.isKeyLocalToAddress(testAddress, key, numOwners);
         assertEquals(isLocal, shouldBeLocal);
      }
   }

   private void addNode(TestTopologyAwareAddress address, String machineId, String rackId, String siteId) {
      addresses.add(address);
      address.setSiteId(siteId);
      address.setRackId(rackId);
      address.setMachineId(machineId);
   }

   private void setAddresses() {
      ch.setCaches(addresses);
      for (int i = 0; i < testAddresses.length; i++) {
         if (testAddresses[i] != null) ((TestAddress)(testAddresses[i])).setName("a" + i);
      }
      log.info("Static addresses: " + Arrays.toString(testAddresses));
   }
}
