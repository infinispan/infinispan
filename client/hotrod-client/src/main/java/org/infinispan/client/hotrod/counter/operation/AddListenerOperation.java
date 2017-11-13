package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * An add listener operation for {@link StrongCounter#addListener(CounterListener)} and {@link
 * WeakCounter#addListener(CounterListener)}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class AddListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;
   private Channel channel;

   public AddListenerOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
                               Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(codec, channelFactory, topologyId, cfg, counterName);
      this.listenerId = listenerId;
      this.server = server;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   protected void executeOperation(Channel channel) {
      this.channel = channel;
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, COUNTER_ADD_LISTENER_REQUEST,
            ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      checkStatus(status);
      if (status != NO_ERROR_STATUS) {
         this.channel = null;
         return false;
      }
      return true;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (server == null) {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      } else {
         channelFactory.fetchChannelAndInvoke(server, this);
      }
   }

   @Override
   public void releaseChannel(Channel channel) {
      if (this.channel != channel) {
         //we aren't using this channel. we can release it
         super.releaseChannel(channel);
      }
   }
}
