package org.infinispan.distribution.ch;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

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
@Test(testName = "distribution.ch.SyncConsistentHashFactoryKeyDistributionTest", groups = "profiling")
public class SyncConsistentHashFactoryKeyDistributionTest extends AbstractInfinispanTest {

   // numbers of nodes to test
   public static final int[] NUM_NODES = {11, 22};
   // numbers of segments to test
   public static final int[] NUM_SEGMENTS = {200, 1000};
   // number of key owners
   public static final int NUM_OWNERS = 2;

   // controls precision + duration of test
   public static final int LOOPS = 1000;

   // confidence intervals to print for any owner
   public static final double[] INTERVALS = { 0.8, 0.9, 1.10, 1.20 };
   // confidence intervals to print for primary owner
   public static final double[] INTERVALS_PRIMARY = { 0.8, 0.9, 1.10, 1.20 };
   // percentiles to print
   public static final double[] PERCENTILES = { .999 };

   protected ConsistentHashFactory<DefaultConsistentHash> createFactory() {
      return SyncConsistentHashFactory.getInstance();
   }

   protected List<Address> createAddresses(int numNodes) {
      ArrayList<Address> addresses = new ArrayList<>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         addresses.add(createSingleAddress(i));
      }
      return addresses;
   }

   public void testDistribution() {
      for (int nn : NUM_NODES) {
         Map<String, Map<Integer, String>> metrics = new TreeMap<>();
         for (int ns : NUM_SEGMENTS) {
            Map<String, String> iterationMetrics = computeMetrics(ns, NUM_OWNERS, nn);
            iterationMetrics.forEach((metricName, metricValue) -> {
               Map<Integer, String> metric = metrics.computeIfAbsent(metricName, k -> new HashMap<>());
               metric.put(ns, metricValue);
            });
         }

         printMetrics(nn, metrics);
      }
   }

   public void testRebalanceDistribution() {
      for (int nn : NUM_NODES) {
         Map<String, Map<Integer, String>> metrics = new TreeMap<>();
         for (int ns : NUM_SEGMENTS) {
            Map<String, String> iterationMetrics = computeMetricsAfterRebalance(ns, NUM_OWNERS, nn);
            iterationMetrics.forEach((metricName, metricValue) -> {
               Map<Integer, String> metric = metrics.computeIfAbsent(metricName, k -> new HashMap<>());
               metric.put(ns, metricValue);
            });
         }

         printMetrics(nn, metrics);
      }
   }


   protected void printMetrics(int nn, Map<String, Map<Integer, String>> metrics) {
      // print the header
      System.out.printf("Distribution for %3d nodes (relative to the average)\n===\n", nn);
      System.out.printf("%35s = ", "Segments");
      for (int numSegment : NUM_SEGMENTS) {
         System.out.printf("%7d", numSegment);
      }
      System.out.println();

      // print each metric for each vnodes setting
      for (Map.Entry<String, Map<Integer, String>> entry : metrics.entrySet()) {
         String metricName = entry.getKey();
         Map<Integer, String> metricValues = entry.getValue();

         System.out.printf("%35s = ", metricName);
         for (int numSegment : NUM_SEGMENTS) {
            System.out.print(metricValues.get(numSegment));
         }
         System.out.println();
      }
      System.out.println();
   }

   protected Map<String, String> computeMetrics(int numSegments, int numOwners, int numNodes) {
      List<Address> members = createAddresses(numNodes);
      Map<String, String> metrics = new HashMap<>();
      long[] distribution = new long[LOOPS * numNodes];
      long[] distributionPrimary = new long[LOOPS * numNodes];
      double[] largestRatio = new double[LOOPS];
      int distIndex = 0;
      ConsistentHashFactory<DefaultConsistentHash> chf = createFactory();
      for (int i = 0; i < LOOPS; i++) {
         DefaultConsistentHash ch = chf.create(numOwners, numSegments, members, null);
         OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
         assertEquals(numSegments * numOwners, stats.sumOwned());
         for (Address node : ch.getMembers()) {
            distribution[distIndex] = stats.getOwned(node);
            distributionPrimary[distIndex] = stats.getPrimaryOwned(node);
            distIndex++;
         }
         largestRatio[i] = getSegmentsPerNodesMinMaxRatio(ch);
      }
      Arrays.sort(distribution);
      Arrays.sort(distributionPrimary);
      Arrays.sort(largestRatio);

      addMetrics(metrics, "Any owner:", numSegments, numOwners, numNodes, distribution, INTERVALS);
      addMetrics(metrics, "Primary:", numSegments, 1, numNodes, distributionPrimary, INTERVALS_PRIMARY);
      addDoubleMetric(metrics, "Segments per node - max/min ratio", largestRatio[largestRatio.length -1]);
      return metrics;
   }

   protected Map<String, String> computeMetricsAfterRebalance(int numSegments, int numOwners, int numNodes) {
      List<Address> members = createAddresses(numNodes);
      Map<String, String> metrics = new HashMap<>();
      long[] distribution = new long[LOOPS * numNodes];
      long[] distributionPrimary = new long[LOOPS * numNodes];
      double[] largestRatio = new double[LOOPS];
      int distIndex = 0;

      ConsistentHashFactory<DefaultConsistentHash> chf = createFactory();
      DefaultConsistentHash ch = chf.create(numOwners, numSegments, members, null);

      // loop leave/join and rebalance
      for (int i = 0; i < LOOPS; i++) {
         // leave
         members.remove(0);
         DefaultConsistentHash rebalancedCH = chf.updateMembers(ch, members, null);
         ch = chf.rebalance(rebalancedCH);
         // join
         Address joiner = createSingleAddress(numNodes + i);
         members.add(joiner);
         rebalancedCH = chf.updateMembers(ch, members, null);
         ch = chf.rebalance(rebalancedCH);
         // stats after rebalance
         OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
         assertEquals(numSegments * numOwners, stats.sumOwned());
         for (Address node : ch.getMembers()) {
            distribution[distIndex] = stats.getOwned(node);
            distributionPrimary[distIndex] = stats.getPrimaryOwned(node);
            distIndex++;
         }
         largestRatio[i] = getSegmentsPerNodesMinMaxRatio(ch);
      }
      Arrays.sort(distribution);
      Arrays.sort(distributionPrimary);
      Arrays.sort(largestRatio);

      addMetrics(metrics, "Any owner:", numSegments, numOwners, numNodes, distribution, INTERVALS);
      addMetrics(metrics, "Primary:", numSegments, 1, numNodes, distributionPrimary, INTERVALS_PRIMARY);
      addDoubleMetric(metrics, "Segments per node - max/min ratio", largestRatio[largestRatio.length -1]);
      return metrics;
   }

   protected void addMetrics(Map<String, String> metrics, String prefix, int numSegments, int numOwners,
                             int numNodes, long[] distribution, double[] intervals) {
      long sum = 0;
      for (long x : distribution) sum += x;
      assertEquals(sum, LOOPS * numOwners * numSegments);
      double mean = (double) sum / numNodes / LOOPS;

      long min = distribution[0];
      long max = distribution[distribution.length - 1];
      addDoubleMetric(metrics, prefix + " min", (double) min / mean);
      addDoubleMetric(metrics, prefix + " max", (double) max / mean);

      double[] intervalProbability = new double[intervals.length];
      int intervalIndex = 0;
      for (int i = 0; i < distribution.length; i++) {
         long x = distribution[i];
         while (x > intervals[intervalIndex] * mean) {
            intervalProbability[intervalIndex] = (double) i / distribution.length;
            intervalIndex++;
            if (intervalIndex >= intervals.length)
               break;
         }
      }
      for (int i = intervalIndex; i < intervals.length; i++) {
         intervalProbability[i] = 1.;
      }

      for (int i = 0; i < intervals.length; i++) {
         if (intervals[i] < 1) {
            addPercentageMetric(metrics, String.format("%s %% < %3.2f", prefix, intervals[i]), intervalProbability[i]);
         } else {
            addPercentageMetric(metrics, String.format("%s %% > %3.2f", prefix, intervals[i]), 1 - intervalProbability[i]);
         }
      }

      double[] percentiles = new double[PERCENTILES.length];
      for (int i = 0; i < PERCENTILES.length; i++) {
         percentiles[i] = (double)distribution[(int) Math.ceil(PERCENTILES[i] * (LOOPS * numNodes + 1))] / mean;
      }
      for (int i = 0; i < PERCENTILES.length; i++) {
         addDoubleMetric(metrics, String.format("%s %5.2f%% percentile", prefix, PERCENTILES[i] * 100), percentiles[i]);
      }
   }

   protected void addDoubleMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%7.3f", value));
   }

   protected void addPercentageMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%6.2f%%", value * 100));
   }

   protected Address createSingleAddress(int nodeIndex) {
      return JGroupsAddress.random();
   }

   protected double getSegmentsPerNodesMinMaxRatio(DefaultConsistentHash ch) {
      int max = 0;
      int min = Integer.MAX_VALUE;
      for (Address addr : ch.getMembers()) {
         int num = ch.getSegmentsForOwner(addr).size();
         max = Math.max(max, num);
         min = Math.min(min, num);
      }
      double d = ((double) max) / min;
      // String result = String.format("min=%d, max=%d, ch=%s, d=%f", min, max, ch, d);
      // System.out.println("segment result = " + result);
      return d;
   }
}
