package org.infinispan.server.hotrod.test;

import java.util.Optional;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestResponseWithPrevious extends TestResponse {
   public final Optional<byte[]> previous;

   protected TestResponseWithPrevious(byte version, long messageId, String cacheName, short clientIntel,
                                      HotRodOperation operation, OperationStatus status, int topologyId,
                                      AbstractTestTopologyAwareResponse topologyResponse, Optional<byte[]> previous) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.previous = previous;
   }
}
