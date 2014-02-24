package org.infinispan.client.hotrod.impl.consistenthash;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.Util;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * @author Galder Zamarreño
 */
public final class SegmentConsistentHash implements ConsistentHash {

   private final Hash hash = new MurmurHash3();
   private SocketAddress[][] segmentOwners;
   private int segmentSize;

   @Override
   public void init(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, int hashSpace) {
      // No-op, parameters are not relevant for this implementation
   }

   public void init(SocketAddress[][] segmentOwners, int numSegments) {
      this.segmentOwners = segmentOwners;
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   @Override
   public SocketAddress getServer(byte[] key) {
      int segmentId = getSegment(key);
      return segmentOwners[segmentId][0];
   }

   private int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return getNormalizedHash(key) / segmentSize;
   }

   @Override
   public int getNormalizedHash(Object object) {
      return Util.getNormalizedHash(object, hash);
   }

}
