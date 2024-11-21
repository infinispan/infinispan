package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import io.netty.channel.Channel;

class AcquireChannelOperation implements ChannelOperation {
   private final CompletableFuture<Channel> cf;

   public AcquireChannelOperation(CompletableFuture<Channel> cf) {
      this.cf = cf;
   }

   @Override
   public void invoke(Channel channel) {
      cf.complete(channel);
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      Exception e = new TimeoutException("Timed out for: " + address);
      e.addSuppressed(cause);
      cf.completeExceptionally(e);
   }
}
