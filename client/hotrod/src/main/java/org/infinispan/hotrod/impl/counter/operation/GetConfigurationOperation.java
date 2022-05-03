package org.infinispan.hotrod.impl.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.decodeConfiguration;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter configuration for {@link CounterManager#getConfiguration(String)}.
 *
 * @since 14.0
 */
public class GetConfigurationOperation extends BaseCounterOperation<CounterConfiguration> {

   public GetConfigurationOperation(OperationContext operationContext, String counterName) {
      super(operationContext, COUNTER_GET_CONFIGURATION_REQUEST, COUNTER_GET_CONFIGURATION_RESPONSE, counterName, false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status != NO_ERROR_STATUS) {
         complete(null);
         return;
      }

      complete(decodeConfiguration(buf::readByte, buf::readLong, () -> ByteBufUtil.readVInt(buf)));
   }
}
