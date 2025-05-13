package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

@Sharable
public class IdleStateHandlerProvider extends ChannelInboundHandlerAdapter {
   private final int minIdle;
   private final ChannelPool channelPool;
   private static final Log log = LogFactory.getLog(IdleStateHandlerProvider.class);

   static final String NAME = "idle-state-handler-provider";

   public IdleStateHandlerProvider(int minIdle, ChannelPool channelPool) {
      this.minIdle = minIdle;
      this.channelPool = channelPool;
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent) {
         if (channelPool.getIdle() > minIdle && ChannelRecord.of(ctx.channel()).isIdle()) {
            log.debugf("Closing idle channel %s", ctx.channel());
            ctx.close();
         }
      } else {
         ctx.fireUserEventTriggered(evt);
      }
   }
}
