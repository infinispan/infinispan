package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @since 14.0
 */
public class IterationEndOperation extends HotRodOperation<IterationEndResponse> {
   private final byte[] iterationId;
   private final Channel channel;

   protected IterationEndOperation(OperationContext operationContext, CacheOptions options, byte[] iterationId, Channel channel) {
      super(operationContext, ITERATION_END_REQUEST, ITERATION_END_RESPONSE, options);
      this.iterationId = iterationId;
      this.channel = channel;
   }

   @Override
   public CompletableFuture<IterationEndResponse> execute() {
      if (!channel.isActive()) {
         throw HOTROD.channelInactive(channel.remoteAddress(), channel.remoteAddress());
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
