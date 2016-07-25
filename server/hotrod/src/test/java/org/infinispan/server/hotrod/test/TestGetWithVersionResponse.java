package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

import java.util.Optional;

/**
 * @author wburns
 * @since 9.0
 */
public class TestGetWithVersionResponse extends TestGetResponse {
   public final long dataVersion;

   protected TestGetWithVersionResponse(byte version, long messageId, String cacheName, short clientIntel,
           OperationResponse operation, OperationStatus status, int topologyId,
           AbstractTestTopologyAwareResponse topologyResponse, Optional<byte[]> data, long dataVersion) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse, data);
      this.dataVersion = dataVersion;
   }
}
