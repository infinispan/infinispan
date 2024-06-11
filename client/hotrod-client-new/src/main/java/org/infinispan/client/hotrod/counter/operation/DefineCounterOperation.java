package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.commons.util.CounterEncodeUtil.encodeConfiguration;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter define operation for {@link CounterManager#defineCounter(String, CounterConfiguration)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class DefineCounterOperation extends BaseCounterOperation<Boolean> {

   private final CounterConfiguration configuration;

   public DefineCounterOperation(String counterName, CounterConfiguration configuration) {
      super(counterName, false);
      this.configuration = configuration;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      encodeConfiguration(configuration, buf::writeByte, buf::writeLong, i -> ByteBufUtil.writeVInt(buf, i));
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      return status == NO_ERROR_STATUS;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_CREATE_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_CREATE_RESPONSE;
   }
}
