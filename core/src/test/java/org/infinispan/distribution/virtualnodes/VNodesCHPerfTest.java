/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.virtualnodes;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.oldch.ConsistentHash;
import org.infinispan.distribution.oldch.ConsistentHashHelper;
import org.infinispan.distribution.oldch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.*;
import static org.testng.Assert.*;

/**
 * Tests the uniformity and performance of the distribution hash algo with virtual nodes.
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 5.0
 */
@Test(testName = "distribution.VNodesCHPerfTest", groups = "manual", enabled = false, description = "Disabled until we can configure Surefire to skip manual tests")
public class VNodesCHPerfTest extends AbstractInfinispanTest {

   private Set<Address> createAddresses(int numNodes) {
      Set<Address> addresses = new HashSet<Address>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         addresses.add(new JGroupsAddress(org.jgroups.util.UUID.randomUUID()));
      }
      return addresses;
   }

   public void testSpeed() {
      int[] numNodes = {1, 2, 3, 4, 10, 100, 1000};
      int iterations = 100000;
      // warmup
      doPerfTest(10, 2, iterations);

      for (int numOwners = 1; numOwners < 5; numOwners++) {
         System.out.println("numOwners=" + numOwners);
         for (int nn: numNodes) {
            Long duration = doPerfTest(nn, numOwners, iterations);
            System.out.println("With "+nn+" cache(s), time to do " + iterations + " lookups was " + Util.prettyPrintTime(TimeUnit.NANOSECONDS.toMillis(duration)));
         }
      }
   }

   private Long doPerfTest(int numNodes, int numOwners, int iterations) {
      ConsistentHash ch = createConsistentHash(numNodes);

      int dummy = 0;
      long start = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
         Object key = i;
         dummy += ch.locate(key, numOwners).size();
      }
      long duration = System.nanoTime() - start;
      assertEquals(dummy, iterations * min(numOwners, numNodes));
      return duration;
   }

   private ConsistentHash createConsistentHash(int numNodes) {
      Set<Address> addresses = createAddresses(numNodes);
      DefaultConsistentHash ch = new DefaultConsistentHash(new MurmurHash3());
      ch.setNumVirtualNodes(10);
      ch.setCaches(addresses);
      return ch;
   }

   public void testDistribution() {
      final int numKeys = 10000;
      final int[] numNodes = {1, 2, 3, 4, 10, 100};

      List<Object> keys = new ArrayList<Object>(numKeys);
      for (int i = 0; i < numKeys; i++) keys.add(i);

      for (int nn : numNodes) {
         doTestDistribution(numKeys, nn, keys);
      }
   }

   private void doTestDistribution(int numKeys, int numNodes, List<Object> keys) {
      ConsistentHash ch = createConsistentHash(numNodes);

      Map<Address, Integer> distribution = new HashMap<Address, Integer>();

      for (Object key : keys) {
         Address a = ch.locate(key, 1).get(0);
         if (distribution.containsKey(a)) {
            int i = distribution.get(a);
            distribution.put(a, i + 1);
         } else {
            distribution.put(a, 1);
         }
      }


      System.out.printf("\nTesting distribution with %d keys, %d nodes\n", numKeys, numNodes);
      //System.out.println("" + distribution);

      // calc numbers
      ArrayList<Integer> counts = new ArrayList<Integer>(distribution.values());
      Collections.sort(counts);

      assertEquals(numNodes, counts.size());
      // instead we add a 0 for all the nonexistent keys in the distribution map and do the calculations
      for (int i = 0; i < numNodes - counts.size(); i++)
         counts.add(0, 0);

      double mean = 0;
      int sum = 0;
      for (Integer count : counts) sum += count;
      assertEquals(sum, numKeys);
      mean = sum / numNodes;

      double variance = 0;
      for (Integer count : counts) variance += (count - mean) * (count - mean);

      double stdDev = sqrt(variance);

      double avgAbsDev = 0;
      for (Integer count : counts) avgAbsDev += abs(count - mean);
      avgAbsDev /= numNodes;

      int median = counts.get(numNodes / 2);
      ArrayList<Integer> medianDevs = new ArrayList<Integer>(numNodes);
      for (Integer count : counts) medianDevs.add(abs(count - median));
      Collections.sort(medianDevs);
      int medianAbsDev = medianDevs.get(numNodes / 2);

      System.out.printf("Mean = %f, median = %d\n", mean, median);
      System.out.printf("Standard deviation = %.3f, or %.3f%%\n", stdDev, stdDev / mean * 100);
      System.out.printf("Average absolute deviation = %.3f, or %.3f%%\n", avgAbsDev, avgAbsDev / mean * 100);
      System.out.printf("Median absolute deviation = %d, or %.3f%%\n", medianAbsDev, (double)medianAbsDev / mean * 100);
   }
}
