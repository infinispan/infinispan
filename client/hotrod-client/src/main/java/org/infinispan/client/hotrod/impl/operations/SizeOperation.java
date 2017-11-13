package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class SizeOperation extends RetryOnFailureOperation<Integer> {

   protected SizeOperation(Codec codec, ChannelFactory channelFactory,
                           byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel, SIZE_REQUEST);
   }

   @Override
   public Integer decodePayload(ByteBuf buf, short status) {
      return ByteBufUtil.readVInt(buf);
   }
}
