package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;

import java.util.Map;

import static org.infinispan.server.hotrod.OperationStatus.Success;

/**
 * @author wburns
 * @since 9.0
 */
public class TestBulkGetResponse extends TestResponse {
   public final Map<byte[], byte[]> bulkData;

   protected TestBulkGetResponse(byte version, long messageId, String cacheName, short clientIntel,
           int topologyId, AbstractTestTopologyAwareResponse topologyResponse, Map<byte[], byte[]> bulkData) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.BulkGetResponse, Success, topologyId, topologyResponse);
      this.bulkData = bulkData;
   }
}
