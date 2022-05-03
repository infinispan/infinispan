package org.infinispan.hotrod.impl.consistenthash;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;

/**
 */
public final class SegmentConsistentHash implements ConsistentHash {

   private static final Log log = LogFactory.getLog(SegmentConsistentHash.class);

   private final Hash hash = MurmurHash3.getInstance();
   private SocketAddress[][] segmentOwners;
   private int numSegments;
   private int segmentSize;

   @Override
   public void init(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, int hashSpace) {
      // No-op, parameters are not relevant for this implementation
   }

   public void init(SocketAddress[][] segmentOwners, int numSegments) {
      this.segmentOwners = segmentOwners;
      this.numSegments = numSegments;
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   @Override
   public SocketAddress getServer(Object key) {
      int segmentId = getSegment(key);
      SocketAddress server = segmentOwners[segmentId][0];

      if (log.isTraceEnabled())
         log.tracef("Found server %s for segment %s of key %s", server, segmentId, Util.toStr(key));
      return server;
   }

   public int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return getNormalizedHash(key) / segmentSize;
   }

   @Override
   public int getNormalizedHash(Object object) {
      return Util.getNormalizedHash(object, hash);
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getSegmentsByServer() {
      Map<SocketAddress, Set<Integer>> map = new HashMap<>();
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         SocketAddress[] owners = segmentOwners[segment];
         for (SocketAddress s : owners) {
            map.computeIfAbsent(s, k -> new HashSet<>()).add(segment);
         }
      }
      return Immutables.immutableMapWrap(map);
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByServer() {
      Map<SocketAddress, Set<Integer>> map = new HashMap<>();
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         SocketAddress[] owners = segmentOwners[segment];
         map.computeIfAbsent(owners[0], k -> new HashSet<>()).add(segment);
      }
      return Immutables.immutableMapWrap(map);
   }

   public int getNumSegments() {
      return numSegments;
   }

   public SocketAddress[][] getSegmentOwners() {
      return segmentOwners;
   }
}
