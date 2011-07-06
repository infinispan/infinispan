/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.*;

/**
 * Tests the uniformity of the distribution hash algo.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "distribution.ConsistentHashPerfTest", groups = "manual", description = "Disabled until we can configure Surefire to skip manual tests")
public class ConsistentHashPerfTest extends AbstractInfinispanTest {

   private Set<Address> createAddresses(int numNodes) {
      Random r = new Random();
      Set<Address> addresses = new HashSet<Address>(numNodes);
      while (addresses.size() < numNodes)
         addresses.add(new JGroupsAddress(new org.jgroups.util.UUID(r.nextLong(), r.nextLong())));
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
      ConsistentHash ch = BaseDistFunctionalTest.createNewConsistentHash(createAddresses(numNodes));
      int dummy = 0;
      long start = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
         Object key = i;
         dummy += ch.locate(key, numOwners).size();
      }
      long duration = System.nanoTime() - start;
      assert dummy == iterations * min(numOwners, numNodes);
      return duration;
   }

   public void testDistribution() {
      final int numKeys = 10000;
      final int[] numNodes = {1, 2, 3, 4, 10, 100, 1000};

      List<Object> keys = new ArrayList<Object>(numKeys);
      for (int i = 0; i < numKeys; i++) keys.add(i);

      for (int nn : numNodes) {
         doTestDistribution(numKeys, nn, keys);
      }
   }

   private void doTestDistribution(int numKeys, int numNodes, List<Object> keys) {
      ConsistentHash ch = BaseDistFunctionalTest.createNewConsistentHash(createAddresses(numNodes));

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

      // When we go to 100 nodes, one or two nodes may not receive any keys and would cause the next assertion to fail
      //assert numNodes == counts.size() : "Only reached " + distribution.size() + " nodes : " + distribution;
      // instead we add a 0 for all the nonexistent keys in the distribution map and do the calculations
      for (int i = 0; i < numNodes - counts.size(); i++)
         counts.add(0, 0);

      double mean = 0;
      int sum = 0;
      for (Integer count : counts) sum += count;
      assert sum == numKeys;
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
      System.out.printf("Median absolute deviation = %d or %.3f%%\n", medianAbsDev, (double)medianAbsDev / mean * 100);
   }
}
