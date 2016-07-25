package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;

import static org.infinispan.server.hotrod.OperationStatus.Success;

/**
 * @author wburns
 * @since 9.0
 */
public class TestSizeResponse extends TestResponse {
   public final long size;

   protected TestSizeResponse(byte version, long messageId, String cacheName, short clientIntel,
           int topologyId, AbstractTestTopologyAwareResponse topologyResponse, long size) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.SizeResponse, Success, topologyId, topologyResponse);
      this.size = size;
   }
}
