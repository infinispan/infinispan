package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class UpdateBloomFilterOperation extends HotRodOperation<Void> implements ChannelOperation {
   private final SocketAddress address;
   private final byte[] bloomBits;

   protected UpdateBloomFilterOperation(Codec codec, ChannelFactory channelFactory,
                                        byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags,
                                        Configuration cfg, SocketAddress address, byte[] bloomBits) {
      super(UPDATE_BLOOM_FILTER_REQUEST, UPDATE_BLOOM_FILTER_RESPONSE, codec, flags, cfg, cacheName, clientTopology, channelFactory);
      this.address = address;
      this.bloomBits = bloomBits;
   }

   @Override
   public CompletableFuture<Void> execute() {
      try {
         channelFactory.fetchChannelAndInvoke(address, this);
      } catch (Exception e) {
         completeExceptionally(e);
      }
      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(null);
   }

   @Override
   public void invoke(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, bloomBits);
      releaseChannel(channel);
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }
}
