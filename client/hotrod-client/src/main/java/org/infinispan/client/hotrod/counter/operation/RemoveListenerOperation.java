package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.counter.api.Handle;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A remove listener operation for {@link Handle#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;

   public RemoveListenerOperation(Codec codec, ChannelFactory transportFactory, AtomicInteger topologyId,
                                  Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.listenerId = listenerId;
      this.server = server;
   }


   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, COUNTER_ADD_LISTENER_REQUEST,
            ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      checkStatus(status);
      return status == NO_ERROR_STATUS;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (server == null) {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      } else {
         channelFactory.fetchChannelAndInvoke(server, this);
      }
   }
}
