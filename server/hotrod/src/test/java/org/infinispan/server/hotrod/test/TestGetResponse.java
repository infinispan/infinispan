package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

import java.util.Optional;

/**
 * @author wburns
 * @since 9.0
 */
public class TestGetResponse extends TestResponse {
   final Optional<byte[]> data;

   protected TestGetResponse(byte version, long messageId, String cacheName, short clientIntel,
           OperationResponse operation, OperationStatus status, int topologyId,
           AbstractTestTopologyAwareResponse topologyResponse, Optional<byte[]> data) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.data = data;
   }
}
