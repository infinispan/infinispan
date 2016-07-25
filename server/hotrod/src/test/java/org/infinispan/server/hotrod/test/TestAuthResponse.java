package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;

import static org.infinispan.server.hotrod.OperationStatus.Success;

/**
 * @author wburns
 * @since 9.0
 */
public class TestAuthResponse extends TestResponse {
   public final boolean complete;
   public final byte[] challenge;

   protected TestAuthResponse(byte version, long messageId, String cacheName, short clientIntel,
           int topologyId, AbstractTestTopologyAwareResponse topologyResponse, boolean complete, byte[] challenge) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.AuthResponse, Success, topologyId, topologyResponse);
      this.complete = complete;
      this.challenge = challenge;
   }
}
