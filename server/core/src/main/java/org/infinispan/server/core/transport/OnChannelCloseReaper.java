package org.infinispan.server.core.transport;

import java.util.function.Consumer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class OnChannelCloseReaper implements GenericFutureListener<Future<? super Void>> {

   private final Consumer<Future<? super Void>> consumer;
   private ChannelFuture channelFuture;

   public OnChannelCloseReaper(Consumer<Future<? super Void>> consumer) {
      this.consumer = consumer;
   }

   @Override
   public void operationComplete(Future<? super Void> future) {
      consumer.accept(future);
   }

   public void registerChannel(Channel channel) {
      channelFuture = channel.closeFuture();
      channelFuture.addListener(this);
   }

   public void dispose() {
      if (channelFuture != null) {
         channelFuture.removeListener(this);
      }
   }
}
