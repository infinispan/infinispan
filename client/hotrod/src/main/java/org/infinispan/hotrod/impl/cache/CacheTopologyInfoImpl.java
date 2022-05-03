package org.infinispan.hotrod.impl.cache;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * @since 14.0
 */
public class CacheTopologyInfoImpl implements CacheTopologyInfo {
   private final Map<SocketAddress, Set<Integer>> segmentsByServer;
   private final Integer numSegments;
   private final Integer topologyId;

   public CacheTopologyInfoImpl(Map<SocketAddress, Set<Integer>> segmentsByServer, Integer numSegments, Integer topologyId) {
      this.segmentsByServer = segmentsByServer;
      this.numSegments = numSegments;
      this.topologyId = topologyId;
   }

   @Override
   public Integer getNumSegments() {
      return numSegments;
   }

   @Override
   public Integer getTopologyId() {
      return topologyId;
   }

   @Override
   public Map<SocketAddress, Set<Integer>> getSegmentsPerServer() {
      return segmentsByServer;
   }

   @Override
   public String toString() {
      return "CacheTopologyInfoImpl{" +
              "segmentsByServer=" + segmentsByServer +
              ", numSegments=" + numSegments +
              ", topologyId=" + topologyId +
              '}';
   }
}
