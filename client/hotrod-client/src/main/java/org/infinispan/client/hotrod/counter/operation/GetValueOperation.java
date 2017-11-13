package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation that returns the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetValueOperation extends BaseCounterOperation<Long> {

   public GetValueOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName) {
      super(codec, channelFactory, topologyId, cfg, counterName);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel, COUNTER_GET_REQUEST);
   }

   @Override
   public Long decodePayload(ByteBuf buf, short status) {
      checkStatus(status);
      assert status == NO_ERROR_STATUS;
      return buf.readLong();
   }
}
