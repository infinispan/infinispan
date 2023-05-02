package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
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

   public IsDefinedOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> clientTopology,
                             Configuration cfg, String counterName) {
      super(COUNTER_IS_DEFINED_REQUEST, COUNTER_IS_DEFINED_RESPONSE, channelFactory, clientTopology, cfg, counterName, false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeHeaderAndCounterName(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(status == NO_ERROR_STATUS);
   }
}
