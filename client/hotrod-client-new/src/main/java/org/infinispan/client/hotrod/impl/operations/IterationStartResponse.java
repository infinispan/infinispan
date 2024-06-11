package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;

import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartResponse {
   private final byte[] iterationId;
   private final SegmentConsistentHash segmentConsistentHash;
   private final int topologyId;
   private final SocketAddress socketAddress;

   IterationStartResponse(byte[] iterationId, SegmentConsistentHash segmentConsistentHash, int topologyId, SocketAddress socketAddress) {
      this.iterationId = iterationId;
      this.segmentConsistentHash = segmentConsistentHash;
      this.topologyId = topologyId;
      this.socketAddress = socketAddress;
   }

   public byte[] getIterationId() {
      return iterationId;
   }

   public SegmentConsistentHash getSegmentConsistentHash() {
      return segmentConsistentHash;
   }

   public SocketAddress getSocketAddress() {
      return socketAddress;
   }

   public int getTopologyId() {
      return topologyId;
   }
}
