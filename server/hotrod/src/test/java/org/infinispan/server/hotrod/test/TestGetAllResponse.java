package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import java.util.Map;

import org.infinispan.server.hotrod.HotRodOperation;

public class TestGetAllResponse extends TestResponse {
   public final Map<byte[], byte[]> data;

   protected TestGetAllResponse(byte version, long messageId, String cacheName, short clientIntel,
                                int topologyId, AbstractTestTopologyAwareResponse topologyResponse,
                                Map<byte[], byte[]> data) {
      super(version, messageId, cacheName, clientIntel, HotRodOperation.GET_ALL, Success, topologyId, topologyResponse);
      this.data = data;
   }
}
