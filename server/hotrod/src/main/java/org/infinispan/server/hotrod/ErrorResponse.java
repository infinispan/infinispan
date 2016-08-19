package org.infinispan.server.hotrod;

/**
 * @author wburns
 * @since 9.0
 */
public class ErrorResponse extends Response {
   protected final String msg;

   ErrorResponse(byte version, long messageId, String cacheName, short clientIntel, OperationStatus status, int topologyId, String msg) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.ErrorResponse, status, topologyId);
      this.msg = msg;
   }

   public String getMsg() {
      return msg;
   }

   @Override
   public String toString() {
      return "ErrorResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", msg=" + msg +
            '}';
   }
}
