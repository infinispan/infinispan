package org.infinispan.server.hotrod.counter.response;

import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET_CONFIGURATION;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * A {@link Response} extension that contains the {@link CounterConfiguration}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterConfigurationResponse extends Response implements CounterResponse {

   private final CounterConfiguration configuration;

   public CounterConfigurationResponse(HotRodHeader header, CounterConfiguration configuration) {
      super(header.getVersion(), header.getMessageId(), header.getCacheName(), header.getClientIntel(),
            COUNTER_GET_CONFIGURATION, OperationStatus.Success, header.getTopologyId());
      this.configuration = configuration;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      encodeConfiguration(configuration, buffer::writeByte, buffer::writeLong,
            value -> ExtendedByteBuf.writeUnsignedInt(value, buffer));
   }
}
