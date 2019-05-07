package org.infinispan.server.hotrod.iteration;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class IterationReaper implements GenericFutureListener<Future<? super Void>> {

   private final IterationManager iterationManager;
   private final String iterationId;
   private ChannelFuture channelFuture;

   public IterationReaper(IterationManager iterationManager, String iterationId) {
      this.iterationManager = iterationManager;
      this.iterationId = iterationId;
   }

   @Override
   public void operationComplete(Future<? super Void> future) {
      iterationManager.close(iterationId);
   }

   public void registerChannel(Channel channel) {
      channelFuture = channel.closeFuture();
      channelFuture.addListener(this);
   }

   public void dispose() {
      channelFuture.removeListener(this);
   }
}
