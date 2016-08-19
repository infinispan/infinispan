package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import org.infinispan.server.hotrod.OperationResponse;

/**
 * @author wburns
 * @since 9.0
 */
public class TestQueryResponse extends TestResponse {
   public final byte[] result;

   protected TestQueryResponse(byte version, long messageId, String cacheName, short clientIntel,
                               int topologyId, AbstractTestTopologyAwareResponse topologyResponse, byte[] result) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.QueryResponse, Success, topologyId, topologyResponse);
      this.result = result;
   }
}
