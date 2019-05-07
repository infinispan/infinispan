package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import org.infinispan.server.hotrod.HotRodOperation;

public class TestIteratorNextResponse extends TestResponse {
   protected TestIteratorNextResponse(byte version, long messageId, String cacheName, short clientIntel,
                                      int topologyId, AbstractTestTopologyAwareResponse topologyResponse) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.ITERATION_NEXT, Success, topologyId, topologyResponse);
   }
}
