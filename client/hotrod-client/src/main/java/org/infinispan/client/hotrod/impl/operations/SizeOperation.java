package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class SizeOperation extends RetryOnFailureOperation<Integer> {

   protected SizeOperation(Codec codec, ChannelFactory channelFactory,
                           byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                           TelemetryService telemetryService) {
      super(SIZE_REQUEST, SIZE_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg, null, telemetryService);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(ByteBufUtil.readVInt(buf));
   }
}
