package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.commons.hash.MurmurHash3;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test (groups = "performance", testName = "client.hotrod.ConsistentHashPerformanceTest")
public class ConsistentHashPerformanceTest {


   public static final int KEY_POOL_SIZE = 1000;
   static List<byte[]> keys = new ArrayList<byte[]>(KEY_POOL_SIZE);

   static {
      Random rnd = new Random();
      for (int i = 0; i < KEY_POOL_SIZE; i++) {
         byte[] bytes = new byte[12];
         rnd.nextBytes(bytes);
         keys.add(bytes);
      }
   }

   private void testConsistentHashSpeed(ConsistentHash ch) {

      int loopSize = 1000000;
      Random rnd = new Random();
      long duration = 0;

      for (int i = 0; i < loopSize; i++) {
         int keyIndex = rnd.nextInt(KEY_POOL_SIZE);

         long start = System.nanoTime();
         SocketAddress server = ch.getServer(keys.get(keyIndex));
         duration += System.nanoTime() - start;

         //just make sure this code is not removed from JIT
         if (server.hashCode() == loopSize) {
            System.out.println("");
         }
      }

      System.out.printf("It took %s millis for consistent hash %s to execute %s operations \n" , TimeUnit.NANOSECONDS.toMillis(duration), ch.getClass().getSimpleName(), loopSize);
   }

   public void testVariousVersion1() {
      ConsistentHashComparisonTest.ConsistentHashV1Old dch2 = new ConsistentHashComparisonTest.ConsistentHashV1Old();
      initConsistentHash(dch2);
      testConsistentHashSpeed(dch2);
   }

   private void initConsistentHash(ConsistentHashComparisonTest.ConsistentHashV1Old dch) {
      int numAddresses = 1500;
      LinkedHashMap<SocketAddress, Set<Integer>> map = new LinkedHashMap<SocketAddress, Set<Integer>>();
      for (int i = 0; i < numAddresses; i++) {
         map.put(new InetSocketAddress(i), Collections.singleton(i * 1000));
      }


      dch.init(map, 2, 10024);
      dch.setHash(new MurmurHash3());
   }
}
