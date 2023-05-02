package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.Handle;

/**
 * A remove listener operation for {@link Handle#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;

   public RemoveListenerOperation(ChannelFactory transportFactory, AtomicReference<ClientTopology> topologyId,
                                  Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(COUNTER_REMOVE_LISTENER_REQUEST, COUNTER_REMOVE_LISTENER_RESPONSE, transportFactory, topologyId, cfg, counterName, false);
      this.listenerId = listenerId;
      this.server = server;
   }


   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeHeaderAndCounterName(buf);
      ByteBufUtil.writeArray(buf, listenerId);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      if (status == NO_ERROR_STATUS) {
         decoder.removeListener(listenerId);
      }
      complete(status == NO_ERROR_STATUS);
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
