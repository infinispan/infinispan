package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
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

   public AddListenerOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId,
                               Configuration cfg, String counterName, byte[] listenerId, SocketAddress server) {
      super(COUNTER_ADD_LISTENER_REQUEST, COUNTER_ADD_LISTENER_RESPONSE, channelFactory, topologyId, cfg, counterName, false);
      this.listenerId = listenerId;
      this.server = server;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   protected void executeOperation(Channel channel) {
      this.channel = channel;
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      if (status != NO_ERROR_STATUS) {
         complete(false);
      } else {
         decoder.addListener(listenerId);
         complete(true);
      }
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
      if (codec.allowOperationsAndEvents()) {
         //we aren't using this channel. we can release it
         super.releaseChannel(channel);
      }
   }

   public void cleanup() {
      // To prevent releasing concurrently from the channel and closing it
      channel.eventLoop().execute(() -> {
         if (log.isTraceEnabled()) {
            log.tracef("Cleanup for %s on %s", this, channel);
         }
         if (!codec.allowOperationsAndEvents()) {
            if (channel.isOpen()) {
               super.releaseChannel(channel);
            }
         }
         HeaderDecoder decoder = channel.pipeline().get(HeaderDecoder.class);
         if (decoder != null) {
            decoder.removeListener(listenerId);
         }
      });
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeHeaderAndCounterName(buf);

      ByteBufUtil.writeArray(buf, listenerId);
   }
}
