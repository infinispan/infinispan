package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.util.Util;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;

/**
 * The ConsistentHashV1 was reimplemented to be more efficient. This test makes sure that the new implementation behaves
 * exactly like the previous implementation.
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional")
public class ConsistentHashComparisonTest {

   public static final int PHISYCAL_NODES = 10;
   public static final int VIRTUAL_NODES = 5;
   private LinkedHashMap<SocketAddress, Set<Integer>> ch;
   CustomRandom customRandom = new CustomRandom();
   ConsistentHashV1Old vOld = new ConsistentHashV1Old(customRandom);
   ConsistentHashV1 vNew = new ConsistentHashV1(customRandom);

   @BeforeTest
   public void init() {

      int hashSpace = Integer.MAX_VALUE;
      int share = hashSpace / PHISYCAL_NODES;
      int virtualNodesSpan = share / VIRTUAL_NODES - 1;

      ch = new LinkedHashMap<SocketAddress, Set<Integer>>();
      for (int i = 0; i < PHISYCAL_NODES; i++) {
         Set<Integer> virtualNodes = new HashSet<Integer>();
         for (int j = 0; j < VIRTUAL_NODES; j++) {
            virtualNodes.add(share * i + virtualNodesSpan * j);
         }
         ch.put(new InetSocketAddress(i), virtualNodes);
      }
      vOld.init(ch, 2, Integer.MAX_VALUE);
      vNew.init(ch, 2, Integer.MAX_VALUE);
   }

   public void testSameValues() {
      for (int i = 0; i < 1000; i++) {
         byte[] key = String.valueOf(i).getBytes();
         assertEquals(vOld.getServer(key), vNew.getServer(key), "int value is " + i);
      }
   }

   public void testSameValues2() {
      Random rnd = new Random();
      for (int i = 0; i < 10000; i++) {
         byte[] key = new byte[i+1];
         rnd.nextBytes(key);
         assertEquals(vOld.getServer(key), vNew.getServer(key), "int value is " + i);
      }
   }

   public void testIsolatedValue() {
      byte[] key = String.valueOf(0).getBytes();
      SocketAddress newOne = vNew.getServer(key);
      SocketAddress oldOne = vOld.getServer(key);
      assertEquals(oldOne, newOne);
   }

   public void testIsolatedValue2() {
      byte[] key = String.valueOf(7).getBytes();
      SocketAddress newOne = vNew.getServer(key);
      SocketAddress oldOne = vOld.getServer(key);
      assertEquals(oldOne, newOne);
   }

   public static class CustomRandom extends Random {

      int callCount = 0;
      int toReturn;

      @Override
      public int nextInt(int val) {
         if (callCount % 2  == 0) {
            toReturn = super.nextInt(val);
         }
         callCount ++;
         return toReturn;
      }
   }

   /**
    * Version one consistent hash function based on {@link org.infinispan.commons.hash.MurmurHash2};
    *
    * @author Mircea.Markus@jboss.com
    * @since 4.1
    */
   static class ConsistentHashV1Old implements ConsistentHash {

      private static final BasicLogger log = BasicLogFactory.getLog(ConsistentHashV1Old.class);

      private final SortedMap<Integer, SocketAddress> positions = new TreeMap<Integer, SocketAddress>();

      private int hashSpace;

      protected Hash hash = new MurmurHash2();

      private int numKeyOwners;

      private final Random rnd;

      public ConsistentHashV1Old(Random rnd) {
         this.rnd = rnd;
      }

      public ConsistentHashV1Old() {
         this(new Random());
      }

      @Override
      public void init(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, int hashSpace) {

         log.infof("Parameters received by CH are: server2Hash: %s, numKeyOwners: %s, hashSpace: %s", servers2Hash,
                   numKeyOwners, hashSpace);

         for (Map.Entry<SocketAddress, Set<Integer>> entry : servers2Hash.entrySet()){
            SocketAddress addr = entry.getKey();
            for (Integer hash : entry.getValue()) {
               SocketAddress prev = positions.put(hash, addr);
               if (prev != null)
                  log.debugf("Adding hash (%d) again, this time for %s. Previously it was associated with: %s", hash, addr, prev);
            }
         }

         log.tracef("Positions (%d entries) are: %s", positions.size(), positions);
         this.hashSpace = hashSpace;
         this.numKeyOwners = numKeyOwners;
      }

      @Override
      public SocketAddress getServer(byte[] key) {
         int keyHashCode = getNormalizedHash(key);
         if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
         int hash = Math.abs(keyHashCode);

         SortedMap<Integer, SocketAddress> candidates = positions.tailMap(hash % hashSpace);
         if (log.isTraceEnabled()) {
            log.tracef("Found possible candidates: %s", candidates);
         }
         return (candidates.size() > 0 ? candidates : positions).entrySet().iterator().next().getValue();
      }

      private SocketAddress getItemAtPosition(int position, SortedMap<Integer, SocketAddress> map) {
         Iterator<Map.Entry<Integer,SocketAddress>> iterator = map.entrySet().iterator();
         for (int i = 0; i < position; i++) {
            iterator.next();
         }
         return iterator.next().getValue();
      }

      public void setHash(Hash hash) {
         this.hash = hash;
      }

      @Override
      public int getNormalizedHash(Object key) {
         return Util.getNormalizedHash(key, hash);
      }

   }
}
