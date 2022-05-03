package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class UpdateBloomFilterOperation extends HotRodOperation<Void> implements ChannelOperation {
   private final SocketAddress address;
   private final byte[] bloomBits;

   protected UpdateBloomFilterOperation(OperationContext operationContext, CacheOptions options,
                                        SocketAddress address, byte[] bloomBits) {
      super(operationContext, UPDATE_BLOOM_FILTER_REQUEST, UPDATE_BLOOM_FILTER_RESPONSE, options);
      this.address = address;
      this.bloomBits = bloomBits;
   }

   @Override
   public CompletableFuture<Void> execute() {
      try {
         operationContext.getChannelFactory().fetchChannelAndInvoke(address, this);
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
