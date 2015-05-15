package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.transport.Transport;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartResponse {
   private final String iterationId;
   private final SegmentConsistentHash segmentConsistentHash;
   private final int topologyId;
   private final Transport transport;

   public IterationStartResponse(String iterationId, SegmentConsistentHash segmentConsistentHash, int topologyId, Transport transport) {
      this.iterationId = iterationId;
      this.segmentConsistentHash = segmentConsistentHash;
      this.topologyId = topologyId;
      this.transport = transport;

   }

   public String getIterationId() {
      return iterationId;
   }

   public SegmentConsistentHash getSegmentConsistentHash() {
      return segmentConsistentHash;
   }

   public Transport getTransport() {
      return transport;
   }

   public int getTopologyId() {
      return topologyId;
   }
}
