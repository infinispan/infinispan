package org.infinispan.server.core.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * A Netty channel handler that allows idle channels to be closed.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
public class IdleStateHandlerProvider extends ChannelInboundHandlerAdapter {

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent) {
         ctx.close();
      }
      ctx.fireUserEventTriggered(evt);
   }
}