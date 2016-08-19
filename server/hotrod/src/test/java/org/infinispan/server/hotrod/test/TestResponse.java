package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;

/**
 * @author wburns
 * @since 9.0
 */
public class TestResponse extends Response {
   public final AbstractTestTopologyAwareResponse topologyResponse;

   protected TestResponse(byte version, long messageId, String cacheName, short clientIntel,
                          OperationResponse operation, OperationStatus status, int topologyId,
                          AbstractTestTopologyAwareResponse topologyResponse) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId);
      this.topologyResponse = topologyResponse;
   }

   @Override
   public String toString() {
      return new StringBuilder().append("Response").append("{")
            .append("version=").append(version)
            .append(", messageId=").append(messageId)
            .append(", operation=").append(operation)
            .append(", status=").append(status)
            .append(", cacheName=").append(cacheName)
            .append(", topologyResponse=").append(topologyResponse)
            .append("}").toString();
   }

   public AbstractTestTopologyAwareResponse asTopologyAwareResponse() {
      if (topologyResponse == null) {
         throw new IllegalStateException("Unexpected response: " + topologyResponse);
      }
      return topologyResponse;
   }
}
