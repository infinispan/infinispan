package org.infinispan.server.hotrod.multimap;

import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;

public class MultimapResponse<V> extends Response {

   private V result;

   public MultimapResponse(HotRodHeader header, HotRodOperation operation, OperationStatus operationStatus, V result) {
      super(header.getVersion(), header.getMessageId(), header.getCacheName(), header.getClientIntel(), operation, operationStatus, header.getTopologyId());
      this.result = result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder().append("MultimapResponse").append("{")
            .append("version=").append(version)
            .append(", messageId=").append(messageId)
            .append(", operation=").append(operation)
            .append(", status=").append(status)
            .append(", result=").append(result);
      return sb.append("}").toString();
   }

   public V getResult() {
      return result;
   }
}
