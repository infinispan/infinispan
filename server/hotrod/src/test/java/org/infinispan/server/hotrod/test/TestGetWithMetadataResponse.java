package org.infinispan.server.hotrod.test;

import java.util.Optional;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestGetWithMetadataResponse extends TestGetWithVersionResponse {
   final long created;
   final int lifespan;
   final long lastUsed;
   final int maxIdle;

   protected TestGetWithMetadataResponse(byte version, long messageId, String cacheName, short clientIntel,
                                         OperationResponse operation, OperationStatus status, int topologyId,
                                         AbstractTestTopologyAwareResponse topologyResponse, Optional<byte[]> data, long dataVersion,
                                         long created, int lifespan, long lastUsed, int maxIdle) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse, data, dataVersion);
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
   }
}
