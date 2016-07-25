package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import java.util.Set;

import org.infinispan.server.hotrod.OperationResponse;

/**
 * @author wburns
 * @since 9.0
 */
public class TestBulkGetKeysResponse extends TestResponse {
   public final Set<byte[]> bulkData;

   protected TestBulkGetKeysResponse(byte version, long messageId, String cacheName, short clientIntel,
                                     int topologyId, AbstractTestTopologyAwareResponse topologyResponse, Set<byte[]> bulkData) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.BulkGetResponse, Success, topologyId, topologyResponse);
      this.bulkData = bulkData;
   }
}
