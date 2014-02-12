package org.infinispan.server.core.transport

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.timeout.IdleStateEvent

/**
 * A Netty channel handler that allows idle channels to be closed.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class IdleStateHandlerProvider extends ChannelInboundHandlerAdapter {


  override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit = {
    System.out.println(evt)
    if (evt.isInstanceOf[IdleStateEvent]) {
      ctx.close
    }
    ctx.fireUserEventTriggered(evt)
  }
}