package org.infinispan.client.hotrod.impl.consistenthash;

import static java.util.Arrays.stream;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;

/**
 * @author Galder Zamarreño
 */
public final class SegmentConsistentHash implements ConsistentHash {

   private static final Log log = LogFactory.getLog(SegmentConsistentHash.class);
   private static final boolean trace = log.isTraceEnabled();

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
      if (trace)
         log.tracef("Find server in segment id %s for key %s", segmentId, Util.toStr(key));

      return segmentOwners[segmentId][0];
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
      IntStream.range(0, segmentOwners.length).forEach(seg -> {
         SocketAddress[] owners = segmentOwners[seg];
         stream(owners).forEach(s -> map.computeIfAbsent(s, k -> new HashSet<>()).add(seg));
      });
      return Immutables.immutableMapWrap(map);
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByServer() {
      Map<SocketAddress, Set<Integer>> map = new HashMap<>();
      IntStream.range(0, segmentOwners.length).forEach(seg -> {
         SocketAddress[] owners = segmentOwners[seg];
         map.computeIfAbsent(owners[0], k -> new HashSet<>()).add(seg);
      });
      return Immutables.immutableMapWrap(map);
   }

   public int getNumSegments() {
      return numSegments;
   }

   public SocketAddress[][] getSegmentOwners() {
      return segmentOwners;
   }
}
