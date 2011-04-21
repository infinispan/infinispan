/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.test.TestingUtil;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests the uniformity of the distribution hash algo.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "distribution.ConsistentHashPerfTest", groups = "manual", enabled = false)
public class ConsistentHashPerfTest extends AbstractInfinispanTest {

   private void addCaches(ConsistentHash ch, int numNodes) {
      Random r = new Random();
      Set<Address> addresses = new HashSet<Address>(numNodes);
      while (addresses.size() < numNodes)
         addresses.add(new JGroupsAddress(new org.jgroups.util.UUID(r.nextLong(), r.nextLong())));
      ch.setCaches(addresses);
   }

   public void testSpeed() {
      int[] numNodes = {1, 10, 100, 1000, 10000};
      int iterations = 100000;
      // warmup
      doPerfTest(numNodes, iterations);
      Map<Integer, Long> performance = doPerfTest(numNodes, iterations);

      for (int i: numNodes) {
         System.out.println("With "+i+" cache(s), time to do " +iterations+ " lookups was " + Util.prettyPrintTime(TimeUnit.NANOSECONDS.toMillis(performance.get(i))));
      }
   }

   private Map<Integer, Long> doPerfTest(int[] numNodes, int iterations) {
      Map<Integer, Long> performance = new HashMap<Integer, Long>();
      for (int nn : numNodes) {
         System.gc();
         TestingUtil.sleepThread(1000);
         ConsistentHash ch = BaseDistFunctionalTest.createNewConsistentHash(null);
         addCaches(ch, nn);
         long start = System.nanoTime();
         Object key = new Object();
         for (int i = 0; i < iterations; i++) ch.locate(key, 1);
         long duration = System.nanoTime() - start;
         performance.put(nn, duration);
      }
      return performance;
   }

   public void testDistribution() {
      final int numKeys = 100000;
      final int numNodes = 100;

      List<Object> keys = new ArrayList<Object>(numKeys);
      ConsistentHash ch = BaseDistFunctionalTest.createNewConsistentHash(null);
      addCaches(ch, numNodes);
      for (int i = 0; i < numKeys; i++) keys.add(UUID.randomUUID());

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

      System.out.println("" + distribution);

      // calc numbers
      float mean = 0;
      int sum = 0;
      for (Integer count : distribution.values()) sum += count;
      assert sum == numKeys;
      mean = sum / distribution.size();
      System.out.println("Mean distribution = " + mean);
      int perfect = numKeys / numNodes;
      System.out.println("Perfect distribution = " + perfect);
      float variance = Math.abs(perfect - mean) * 100 / perfect;
      System.out.println("Variance % = " + variance);
   }
}
