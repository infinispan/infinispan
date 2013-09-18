package org.infinispan.distribution;

import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
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

   private List<Address> createAddresses(int numNodes) {
      Random r = new Random();
      List<Address> addresses = new ArrayList<Address>(numNodes);
      while (addresses.size() < numNodes)
         addresses.add(new JGroupsAddress(new org.jgroups.util.UUID(r.nextLong(), r.nextLong())));
      return addresses;
   }

   private ConsistentHash createNewConsistentHash(List<Address> servers) {
      try {
         // TODO Revisit after we have replaced the CH with the CHFactory in the configuration
         return new DefaultConsistentHashFactory().create(new org.infinispan.commons.hash.MurmurHash3(), 2, 10,
               servers, null);
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
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
      ConsistentHash ch = createNewConsistentHash(createAddresses(numNodes));
      int dummy = 0;
      long start = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
         Object key = i;
         dummy += ch.locateOwners(key).size();
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
      ConsistentHash ch = createNewConsistentHash(createAddresses(numNodes));

      Map<Address, Integer> distribution = new HashMap<Address, Integer>();

      for (Object key : keys) {
         Address a = ch.locateOwners(key).get(0);
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
