package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import org.infinispan.server.hotrod.HotRodOperation;

/**
 * @author wburns
 * @since 9.0
 */
public class TestSizeResponse extends TestResponse {
   public final long size;

   protected TestSizeResponse(byte version, long messageId, String cacheName, short clientIntel,
                              int topologyId, AbstractTestTopologyAwareResponse topologyResponse, long size) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.SIZE, Success, topologyId, topologyResponse);
      this.size = size;
   }
}
