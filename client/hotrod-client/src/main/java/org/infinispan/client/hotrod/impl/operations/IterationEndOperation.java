package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
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
      super(codec, flags, cfg, cacheName, topologyId, channelFactory);
      this.iterationId = iterationId;
      this.channel = channel;
   }

   @Override
   public CompletableFuture<IterationEndResponse> execute() {
      if (!channel.isActive()) {
         throw log.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }
      HeaderParams header = headerParams(ITERATION_END_REQUEST);
      scheduleRead(channel, header);
      sendArrayOperation(channel, header, iterationId);
      return this;
   }

   @Override
   public IterationEndResponse decodePayload(ByteBuf buf, short status) {
      return new IterationEndResponse(status);
   }
}
