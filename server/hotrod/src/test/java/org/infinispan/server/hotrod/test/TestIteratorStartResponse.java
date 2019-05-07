package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import org.infinispan.server.hotrod.HotRodOperation;

public class TestIteratorStartResponse extends TestResponse {
   public final String iteratorId;

   protected TestIteratorStartResponse(byte version, long messageId, String cacheName, short clientIntel,
                                       int topologyId, AbstractTestTopologyAwareResponse topologyResponse, String iteratorId) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.ITERATION_START, Success, topologyId, topologyResponse);
      this.iteratorId = iteratorId;
   }
}
