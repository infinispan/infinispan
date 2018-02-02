package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationEndOperation extends HotRodOperation<IterationEndResponse> {
   private static final Log log = LogFactory.getLog(IterationEndOperation.class);

   private final byte[] iterationId;
   private final Channel channel;

   protected IterationEndOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName,
                                   AtomicInteger topologyId, byte[] iterationId, ChannelFactory channelFactory,
                                   Channel channel) {
      super(ITERATION_END_REQUEST, ITERATION_END_RESPONSE, codec, flags, cfg, cacheName, topologyId, channelFactory);
      this.iterationId = iterationId;
      this.channel = channel;
   }

   @Override
   public CompletableFuture<IterationEndResponse> execute() {
      if (!channel.isActive()) {
         throw log.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }
      scheduleRead(channel);
      sendArrayOperation(channel, iterationId);
      releaseChannel(channel);
      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(new IterationEndResponse(status));
   }
}
