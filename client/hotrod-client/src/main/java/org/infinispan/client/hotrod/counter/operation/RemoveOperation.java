package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A counter operation for {@link CounterManager#remove(String)}, {@link StrongCounter#remove()} and {@link
 * WeakCounter#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveOperation extends BaseCounterOperation<Void> {
   public RemoveOperation(ChannelFactory transportFactory, AtomicReference<ClientTopology> topologyId,
                          Configuration cfg, String counterName, boolean useConsistentHash) {
      super(COUNTER_REMOVE_REQUEST, COUNTER_REMOVE_RESPONSE, transportFactory, topologyId, cfg, counterName, useConsistentHash);
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
