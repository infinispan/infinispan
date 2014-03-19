package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static java.lang.Math.sqrt;
import static org.testng.Assert.assertEquals;

/**
 * Tests the uniformity of the SyncConsistentHashFactory algorithm, which is very similar to the 5.1
 * default consistent hash algorithm virtual nodes.
 *
 * <p>This test assumes that key hashes are random and follow a uniform distribution  so a key has the same chance
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
 * @since 5.2
 */
@Test(testName = "distribution.ch.SyncConsistentHashFactoryKeyDistributionTest", groups = "manual", description = "See the results in vnodes_key_dist.txt")
public class SyncConsistentHashFactoryKeyDistributionTest extends AbstractInfinispanTest {

   // numbers of nodes to test
   public static final int[] NUM_NODES = {2, 4, 8, 16, 32, 48, 64, 128, 256};
   // numbers of virtual nodes to test
   public static final int[] NUM_SEGMENTS = {64, 256, 1024, 4096, 163841};
   // number of key owners
   public static final int NUM_OWNERS = 2;

   // controls precision + duration of test
   public static final int LOOPS = 2000;
   // confidence intervals to print for any owner
   public static final double[] INTERVALS = { 1.25 };
   // confidence intervals to print for primary owner
   public static final double[] INTERVALS_PRIMARY = { 1.5 };
   // percentiles to print
   public static final double[] PERCENTILES = { .999 };

   private DefaultConsistentHash createConsistentHash(int numSegments, int numOwners, int numNodes) {
      MurmurHash3 hash = new MurmurHash3();
      SyncConsistentHashFactory chf = new SyncConsistentHashFactory();
      DefaultConsistentHash ch = chf.create(hash, numOwners, numSegments, createAddresses(numNodes), null);
      return ch;
   }

   private List<Address> createAddresses(int numNodes) {
      ArrayList<Address> addresses = new ArrayList<Address>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         addresses.add(new IndexedJGroupsAddress(org.jgroups.util.UUID.randomUUID(), i));
      }
      return addresses;
   }

   public void testDistribution() {
      for (int nn : NUM_NODES) {
         Map<String, Map<Integer, String>> metrics = new TreeMap<String, Map<Integer, String>>();
         for (int ns : NUM_SEGMENTS) {
            for (Map.Entry<String, String> entry : computeMetrics(ns, NUM_OWNERS, nn).entrySet()) {
               String metricName = entry.getKey();
               String metricValue = entry.getValue();
               Map<Integer, String> metric = metrics.get(metricName);
               if (metric == null) {
                  metric = new HashMap<Integer, String>();
                  metrics.put(metricName, metric);
               }
               metric.put(ns, metricValue);
            };
         }

         printMetrics(nn, metrics);
      }
   }

   private void printMetrics(int nn, Map<String, Map<Integer, String>> metrics) {
      // print the header
      System.out.printf("Distribution for %3d nodes\n===\n", nn);
      System.out.printf("%54s = ", "Segments");
      for (int i = 0; i < NUM_SEGMENTS.length; i++) {
         System.out.printf("%7d", NUM_SEGMENTS[i]);
      }
      System.out.println();

      // print each metric for each vnodes setting
      for (Map.Entry<String, Map<Integer, String>> entry : metrics.entrySet()) {
         String metricName = entry.getKey();
         Map<Integer, String> metricValues = entry.getValue();

         System.out.printf("%54s = ", metricName);
         for (int i = 0; i < NUM_SEGMENTS.length; i++) {
            System.out.print(metricValues.get(NUM_SEGMENTS[i]));
         }
         System.out.println();
      }
      System.out.println();
   }

   private Map<String, String> computeMetrics(int numSegments, int numOwners, int numNodes) {
      Map<String, String> metrics = new HashMap<String, String>();
      long[] distribution = new long[LOOPS * numNodes];
      long[] distributionPrimary = new long[LOOPS * numNodes];
      int distIndex = 0;
      for (int i = 0; i < LOOPS; i++) {
         DefaultConsistentHash ch = createConsistentHash(numSegments, numOwners, numNodes);
         OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
         for (Address node : ch.getMembers()) {
            distribution[distIndex] = stats.getOwned(node);
            distributionPrimary[distIndex] = stats.getPrimaryOwned(node);
            distIndex++;
         }
      }
      Arrays.sort(distribution);
      Arrays.sort(distributionPrimary);

      addMetrics(metrics, "Any owner:", numSegments, numOwners, numNodes, distribution, INTERVALS);
      addMetrics(metrics, "Primary:", numSegments, 1, numNodes, distributionPrimary, INTERVALS_PRIMARY);
      return metrics;
   }

   private void addMetrics(Map<String, String> metrics, String prefix, int numSegments, int numOwners,
                           int numNodes, long[] distribution, double[] intervals) {
      double mean = 0;
      long sum = 0;
      for (long x : distribution) sum += x;
      assertEquals(sum, (long) LOOPS * numOwners * numSegments);
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
