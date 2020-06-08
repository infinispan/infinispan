package org.infinispan.client.hotrod.impl.consistenthash;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.Util;
import org.jboss.logging.BasicLogger;

/**
 * Version 2 of the ConsistentHash function.  Uses MurmurHash3.
 *
 * @author manik
 * @see org.infinispan.commons.hash.MurmurHash3
 * @since 5.0
 */
public class ConsistentHashV2 implements ConsistentHash {

   private static final BasicLogger log = LogFactory.getLog(ConsistentHashV2.class);

   private final SortedMap<Integer, SocketAddress> positions = new TreeMap<Integer, SocketAddress>();

   private volatile int[] hashes;
   private volatile SocketAddress[] addresses;

   private int hashSpace;
   private boolean hashSpaceIsMaxInt;

   protected Hash hash = MurmurHash3.getInstance();

   private int numKeyOwners;

   private final Random rnd;


   public ConsistentHashV2(Random rnd) {
      this.rnd = rnd;
   }

   public ConsistentHashV2() {
      this(new Random());
   }

   @Override
   public void init(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, int hashSpace) {
      for (Map.Entry<SocketAddress, Set<Integer>> entry : servers2Hash.entrySet()) {
         SocketAddress addr = entry.getKey();
         for (Integer hash : entry.getValue()) {
            SocketAddress prev = positions.put(hash, addr);
            if (prev != null)
               log.debugf("Adding hash (%d) again, this time for %s. Previously it was associated with: %s", hash, addr, prev);
         }
      }

      int hashWheelSize = positions.size();
      log.tracef("Positions (%d entries) are: %s", hashWheelSize, positions);

      hashes = new int[hashWheelSize];
      Iterator<Integer> it = positions.keySet().iterator();
      for (int i = 0; i < hashWheelSize; i++) {
         hashes[i] = it.next();
      }
      addresses = positions.values().toArray(new SocketAddress[hashWheelSize]);

      this.hashSpace = hashSpace;

      // This is true if we're talking to an instance of Infinispan 5.2 or newer.
      this.hashSpaceIsMaxInt = hashSpace == Integer.MAX_VALUE;

      this.numKeyOwners = numKeyOwners;
   }

   @Override
   public SocketAddress getServer(Object key) {
      int normalisedHashForKey;
      if (hashSpaceIsMaxInt) {
         normalisedHashForKey = getNormalizedHash(key);
         if (normalisedHashForKey == Integer.MAX_VALUE) normalisedHashForKey = 0;
      } else {
         normalisedHashForKey = getNormalizedHash(key) % hashSpace;
      }

      int mainOwner = getHashIndex(normalisedHashForKey);

      int indexToReturn = mainOwner % hashes.length;

      return addresses[indexToReturn];
   }

   private int getHashIndex(int normalisedHashForKey) {
      int result = Arrays.binarySearch(hashes, normalisedHashForKey);
      if (result >= 0) {//the normalisedHashForKey has an exact match in the hashes array
         return result;
      } else {
         //see javadoc for Arrays.binarySearch, @return tag in particular
         if (result == (-hashes.length - 1)) {
            return 0;
         } else {
            return -result - 1;
         }
      }
   }

   private int getIndex() {
      return rnd.nextInt(Math.min(numKeyOwners, positions.size()));
   }

   public void setHash(Hash hash) {
      this.hash = hash;
   }

   @Override
   public final int getNormalizedHash(Object object) {
      return Util.getNormalizedHash(object, hash);
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getSegmentsByServer() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByServer() {
      throw new UnsupportedOperationException();
   }

}
