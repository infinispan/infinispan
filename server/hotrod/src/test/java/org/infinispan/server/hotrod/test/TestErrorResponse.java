package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestErrorResponse extends TestResponse {
   public final String msg;

   protected TestErrorResponse(byte version, long messageId, String cacheName, short clientIntel,
                               OperationStatus status, int topologyId, AbstractTestTopologyAwareResponse topologyResponse, String msg) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.ErrorResponse, status, topologyId, topologyResponse);
      this.msg = msg;
   }
}
