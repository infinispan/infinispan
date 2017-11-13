package org.infinispan.server.hotrod.counter.response;

import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;

import io.netty.buffer.ByteBuf;

/**
 * A counter response for operations that return the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterValueResponse extends Response implements CounterResponse {

   private final long value;

   public CounterValueResponse(HotRodHeader header, long value) {
      super(header.getVersion(), header.getMessageId(), header.getCacheName(), header.getClientIntel(), header.getOp(),
            OperationStatus.Success, header.getTopologyId());
      this.value = value;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      buffer.writeLong(value);
   }

   @Override
   public String toString() {
      return "CounterValueResponse{" +
            "version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", clientIntel=" + clientIntel +
            ", operation=" + operation +
            ", status=" + status +
            ", topologyId=" + topologyId +
            ", value=" + value +
            '}';
   }
}
