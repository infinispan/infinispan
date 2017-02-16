package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import org.infinispan.server.hotrod.HotRodOperation;

/**
 * @author wburns
 * @since 9.0
 */
public class TestAuthResponse extends TestResponse {
   public final boolean complete;
   public final byte[] challenge;

   protected TestAuthResponse(byte version, long messageId, String cacheName, short clientIntel,
                              int topologyId, AbstractTestTopologyAwareResponse topologyResponse, boolean complete, byte[] challenge) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.AUTH, Success, topologyId, topologyResponse);
      this.complete = complete;
      this.challenge = challenge;
   }
}
