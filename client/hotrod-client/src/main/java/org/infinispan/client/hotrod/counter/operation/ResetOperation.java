package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A counter operation for {@link StrongCounter#reset()} and {@link WeakCounter#reset()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class ResetOperation extends BaseCounterOperation<Void> {

   public ResetOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId, Configuration cfg,
         String counterName, boolean useConsistentHash) {
      super(COUNTER_RESET_REQUEST, COUNTER_RESET_RESPONSE, channelFactory, topologyId, cfg, counterName, useConsistentHash);
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
      checkStatus(status);
      complete(null);
   }
}
