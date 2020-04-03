package org.infinispan.client.hotrod.test;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class NoopChannelOperation extends CompletableFuture<Channel> implements ChannelOperation {
   @Override
   public ChannelFuture invoke(Channel channel) {
      complete(channel);
      return null;
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }
}
