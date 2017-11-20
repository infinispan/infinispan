package org.infinispan.server.hotrod.counter.response;

import static org.infinispan.server.core.transport.VInt.write;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeString;

import java.util.Collection;

import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;

import io.netty.buffer.ByteBuf;

/**
 * A {@link Response} extension that contains the a collection of counter names.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterNamesResponse extends Response implements CounterResponse {

   private final Collection<String> counterNames;

   public CounterNamesResponse(HotRodHeader header, Collection<String> counterNames) {
      super(header.getVersion(), header.getMessageId(), header.getCacheName(), header.getClientIntel(), header.getOp(),
            OperationStatus.Success, header.getTopologyId());
      this.counterNames = counterNames;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      write(buffer, counterNames.size());
      counterNames.forEach(s -> writeString(s, buffer));
   }
}
