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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static java.lang.Math.sqrt;
import static org.testng.Assert.assertEquals;

/**
 * Tests the uniformity of the consistent hash algorithm with virtual nodes.
 *
 * <p>This test assumes that key hashes are random and follow a uniform distribution - so a key has the same chance
 * to land on each one of the 2^31 positions on the hash wheel.
 *
 * <p>The output should stay pretty much the same between runs, so I added and example output here: vnodes_key_dist.txt.
 *
 * <p>Notes about the test output:
 * <ul>
 * <li>{@code P(p)} is the probability of proposition {@code p} being true
 * <li>In the "Primary" rows {@code mean == total_keys / num_nodes} (each key has only one primary owner),
 * but in the "Any owner" rows {@code mean == total_keys / num_nodes * num_owners} (each key is stored on
 * {@code num_owner} nodes).
 * </ul>
 * @author Dan Berindei
 * @since 5.1.1
 */
@Test(testName = "distribution.VNodesKeyDistributionTest", groups = "manual", enabled = false, description = "See the results in vnodes_key_dist.txt")
public class VNodesKeyDistributionTest extends AbstractInfinispanTest {

   // numbers of nodes to test
   public static final int[] NUM_NODES = {2, 4, 8, 16, 32, 48, 64, 128, 256};
   // numbers of virtual nodes to test
   public static final int[] NUM_VIRTUAL_NODES = {1, 4, 16, 32, 48, 64, 96, 128};
   // number of key owners
   public static final int NUM_OWNERS = 2;

   // controls precision + duration of test
   public static final int LOOPS = 10000;
   // confidence intervals to print for any owner
   public static final double[] INTERVALS = { 1.25 };
   // confidence intervals to print for primary owner
   public static final double[] INTERVALS_PRIMARY = { 1.5 };
   // percentiles to print
   public static final double[] PERCENTILES = { .999 };

   private TransparentDefaultConsistentHash createConsistentHash(int numNodes, int numVirtualNodes) {
      MurmurHash3 hash = new MurmurHash3();
      TransparentDefaultConsistentHash ch = new TransparentDefaultConsistentHash();
      ch.setHashFunction(hash);
      ch.setNumVirtualNodes(numVirtualNodes);
      ch.setCaches(createAddresses(numNodes));
      return ch;
   }

   private Set<Address> createAddresses(int numNodes) {
      Set<Address> addresses = new HashSet<Address>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         addresses.add(new IndexedJGroupsAddress(org.jgroups.util.UUID.randomUUID(), i));
      }
      return addresses;
   }

   public void testDistribution() {
      for (int nn : NUM_NODES) {
         Map<String, Map<Integer, String>> metrics = new TreeMap<String, Map<Integer, String>>();
         for (int vn : NUM_VIRTUAL_NODES) {
            for (Map.Entry<String, String> entry : computeMetrics(nn, vn, NUM_OWNERS).entrySet()) {
               String metricName = entry.getKey();
               String metricValue = entry.getValue();
               Map<Integer, String> metric = metrics.get(metricName);
               if (metric == null) {
                  metric = new HashMap<Integer, String>();
                  metrics.put(metricName, metric);
               }
               metric.put(vn, metricValue);
            };
         }

         printMetrics(nn, metrics);
      }
   }

   private void printMetrics(int nn, Map<String, Map<Integer, String>> metrics) {
      // print the header
      System.out.printf("Distribution for %3d nodes\n===\n", nn);
      System.out.printf("%-54s = ", "Virtual nodes");
      for (int i = 0; i < NUM_VIRTUAL_NODES.length; i++) {
         System.out.printf("%7d", NUM_VIRTUAL_NODES[i]);
      }
      System.out.println();

      // print each metric for each vnodes setting
      for (Map.Entry<String, Map<Integer, String>> entry : metrics.entrySet()) {
         String metricName = entry.getKey();
         Map<Integer, String> metricValues = entry.getValue();

         System.out.printf("%-54s = ", metricName);
         for (int i = 0; i < NUM_VIRTUAL_NODES.length; i++) {
            System.out.print(metricValues.get(NUM_VIRTUAL_NODES[i]));
         }
         System.out.println();
      }
      System.out.println();
   }

   private Map<String, String> computeMetrics(int numNodes, int numVirtualNodes, int numOwners) {
      Map<String, String> metrics = new HashMap<String, String>();
      long[] distribution = new long[LOOPS * numNodes];
      long[] distributionPrimary = new long[LOOPS * numNodes];
      for (int i = 0; i < LOOPS; i++) {
         TransparentDefaultConsistentHash ch = createConsistentHash(numNodes, numVirtualNodes);
         long[] dist = computeDistribution(numNodes, numOwners, ch);
         System.arraycopy(dist, 0, distribution, i * numNodes, numNodes);
         long[] distPrimary = computeDistribution(numNodes, 1, ch);
         System.arraycopy(distPrimary, 0, distributionPrimary, i * numNodes, numNodes);
      }
      Arrays.sort(distribution);
      Arrays.sort(distributionPrimary);

      addMetrics(metrics, "Any owner:", numNodes, numOwners, distribution, INTERVALS);
      addMetrics(metrics, "Primary:", numNodes, 1, distributionPrimary, INTERVALS_PRIMARY);
      return metrics;
   }

   private void addMetrics(Map<String, String> metrics, String prefix, int numNodes, int numOwners, long[] distribution, double[] intervals) {
      double mean = 0;
      long sum = 0;
      for (long x : distribution) sum += x;
      assertEquals(sum, LOOPS * numOwners * (long)Integer.MAX_VALUE);
      mean = sum / numNodes / LOOPS;

      double variance = 0;
      for (long x : distribution) variance += (x - mean) * (x - mean);

      double stdDev = sqrt(variance);
      // metrics.put(prefix + " relative standard deviation", stdDev / mean);

      long max = distribution[distribution.length - 1];
      // metrics.put(prefix + " min", (double) min / mean);
      addDoubleMetric(metrics, prefix + " max(num_keys(node)/mean)", (double) max / mean);

      double[] intervalConfidence = new double[intervals.length];
      int intervalIndex = 0;
      for (int i = 0; i < distribution.length; i++) {
         long x = distribution[i];
         if (x > intervals[intervalIndex] * mean) {
            intervalConfidence[intervalIndex] = (double) i / distribution.length;
            intervalIndex++;
            if (intervalIndex >= intervals.length)
               break;
         }
      }
      for (int i = intervalIndex; i < intervals.length; i++) {
         intervalConfidence[i] = 1.;
      }

      for (int i = 0; i < intervals.length; i++) {
         if (intervals[i] < 1) {
            addPercentageMetric(metrics, String.format("%s P(num_keys(node) < %3.2f * mean)", prefix, intervals[i]), intervalConfidence[i]);
         } else {
            addPercentageMetric(metrics, String.format("%s P(num_keys(node) > %3.2f * mean)", prefix, intervals[i]), 1 - intervalConfidence[i]);
         }
      }

      double[] percentiles = new double[PERCENTILES.length];
      for (int i = 0; i < PERCENTILES.length; i++) {
         percentiles[i] = (double)distribution[(int) Math.ceil(PERCENTILES[i] * (LOOPS * numNodes + 1))] / mean;
      }
      for (int i = 0; i < PERCENTILES.length; i++) {
         addDoubleMetric(metrics, String.format("%s P(num_keys(node) <= x * mean) = %5.2f%% => x", prefix, PERCENTILES[i] * 100), percentiles[i]);
      }
   }

   private void addDoubleMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%7.3f", value));
   }

   private void addPercentageMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%6.2f%%", value * 100));
   }

   private long[] computeDistribution(int numNodes, int numOwners, TransparentDefaultConsistentHash ch) {
      long[] distribution = new long[numNodes];

      int[] hashPositions = ch.getHashPositions();
      for (int i = 0; i < hashPositions.length; i++) {
         int hashPosition = hashPositions[i];
         int previousHashPosition = i > 0 ? hashPositions[i - 1] : hashPositions[hashPositions.length - 1];
         List<Address> owners = ch.locateHash(hashPosition, numOwners);

         for (Address a : owners) {
            IndexedJGroupsAddress ma = (IndexedJGroupsAddress) a;
            distribution[ma.nodeIndex] += hashPosition > previousHashPosition
                  ? hashPosition - previousHashPosition
                  : Integer.MAX_VALUE + hashPosition - previousHashPosition;
         }
      }
      return distribution;
   }
}

/**
 * We extend DefaultConsistentHash because we need access to its {@code protected} internals
 */
class TransparentDefaultConsistentHash extends DefaultConsistentHash {
   public int[] getHashPositions() { return positionKeys; }

   // This method mirrors DefaultConsistentHash.locate(), but it takes an already-normalized hash as input
   public List<Address> locateHash(int normalizedHash, int replCount) {
      final int actualReplCount = Math.min(replCount, caches.size());
      final List<Address> owners = new ArrayList<Address>(actualReplCount);
      final boolean virtualNodesEnabled = isVirtualNodesEnabled();

      for (Iterator<Address> it = getPositionsIterator(normalizedHash); it.hasNext();) {
         Address a = it.next();
         // if virtual nodes are enabled we have to avoid duplicate addresses
         boolean isDuplicate = virtualNodesEnabled && owners.contains(a);
         if (!isDuplicate) {
            owners.add(a);
            if (owners.size() >= actualReplCount)
               return owners;
         }
      }

      // might return < replCount owners if there aren't enough nodes in the list
      return owners;
   }
}

/**
 * We extend JGroupsAddress to make mapping an address to a node easier.
 */
class IndexedJGroupsAddress extends JGroupsAddress {
   final int nodeIndex;

   IndexedJGroupsAddress(org.jgroups.Address address, int nodeIndex) {
      super(address);
      this.nodeIndex = nodeIndex;
   }
}
