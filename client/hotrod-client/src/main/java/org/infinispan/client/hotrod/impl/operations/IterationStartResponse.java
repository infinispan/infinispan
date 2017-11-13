package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;

import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartResponse {
   private final byte[] iterationId;
   private final SegmentConsistentHash segmentConsistentHash;
   private final int topologyId;
   private final Channel channel;

   IterationStartResponse(byte[] iterationId, SegmentConsistentHash segmentConsistentHash, int topologyId, Channel channel) {
      this.iterationId = iterationId;
      this.segmentConsistentHash = segmentConsistentHash;
      this.topologyId = topologyId;
      this.channel = channel;
   }

   public byte[] getIterationId() {
      return iterationId;
   }

   public SegmentConsistentHash getSegmentConsistentHash() {
      return segmentConsistentHash;
   }

   public Channel getChannel() {
      return channel;
   }

   public int getTopologyId() {
      return topologyId;
   }
}
