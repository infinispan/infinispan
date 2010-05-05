package org.infinispan.server.core.transport

import org.jboss.netty.handler.timeout.{IdleStateEvent, IdleStateAwareChannelHandler}
import org.jboss.netty.channel.{ChannelHandlerContext => NettyChannelHandlerContext}
/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class IdleStateHandlerProvider extends IdleStateAwareChannelHandler {

   override def channelIdle(nCtx: NettyChannelHandlerContext, e: IdleStateEvent) {
      nCtx.getChannel.disconnect
      super.channelIdle(nCtx, e)
   }

}