package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link CounterManager#isDefined(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class IsDefinedOperation extends BaseCounterOperation<Boolean> {

   public IsDefinedOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
                             Configuration cfg, String counterName) {
      super(codec, channelFactory, topologyId, cfg, counterName);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel, COUNTER_IS_DEFINED_REQUEST);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      return status == NO_ERROR_STATUS;
   }
}
