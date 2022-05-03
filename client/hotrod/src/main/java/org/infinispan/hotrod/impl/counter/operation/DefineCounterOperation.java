package org.infinispan.hotrod.impl.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter define operation for {@link CounterManager#defineCounter(String, CounterConfiguration)}.
 *
 * @since 14.0
 */
public class DefineCounterOperation extends BaseCounterOperation<Boolean> {

   private final CounterConfiguration configuration;

   public DefineCounterOperation(OperationContext operationContext, String counterName, CounterConfiguration configuration) {
      super(operationContext, COUNTER_CREATE_REQUEST, COUNTER_CREATE_RESPONSE, counterName, false);
      this.configuration = configuration;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 28);
      encodeConfiguration(configuration, buf::writeByte, buf::writeLong, i -> ByteBufUtil.writeVInt(buf, i));
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      complete(status == NO_ERROR_STATUS);
   }
}
