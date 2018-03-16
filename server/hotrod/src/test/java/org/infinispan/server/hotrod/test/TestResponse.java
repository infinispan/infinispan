package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestResponse {
   public final AbstractTestTopologyAwareResponse topologyResponse;
   public final byte version;
   public final long messageId;
   public final String cacheName;
   public final short clientIntel;
   public final HotRodOperation operation;
   public final OperationStatus status;
   public final int topologyId;

   protected TestResponse(byte version, long messageId, String cacheName, short clientIntel,
                          HotRodOperation operation, OperationStatus status, int topologyId,
                          AbstractTestTopologyAwareResponse topologyResponse) {
      this.version = version;
      this.messageId = messageId;
      this.cacheName = cacheName;
      this.clientIntel = clientIntel;
      this.operation = operation;
      this.status = status;
      this.topologyId = topologyId;
      this.topologyResponse = topologyResponse;
   }

   public AbstractTestTopologyAwareResponse asTopologyAwareResponse() {
      if (topologyResponse == null) {
         throw new IllegalStateException("Unexpected response: " + topologyResponse);
      }
      return topologyResponse;
   }

   public byte getVersion() {
      return version;
   }

   public long getMessageId() {
      return messageId;
   }

   public String getCacheName() {
      return cacheName;
   }

   public short getClientIntel() {
      return clientIntel;
   }

   public HotRodOperation getOperation() {
      return operation;
   }

   public OperationStatus getStatus() {
      return status;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "Response{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            '}';
   }
}
