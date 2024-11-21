package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

/**
 * A counter operation that returns the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetValueOperation extends BaseCounterOperation<Long> {

   public GetValueOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId,
         Configuration cfg, String counterName, boolean useConsistentHash) {
      super(COUNTER_GET_REQUEST, COUNTER_GET_RESPONSE, channelFactory, topologyId, cfg, counterName, useConsistentHash);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      assert status == NO_ERROR_STATUS;
      complete(buf.readLong());
   }
}
