package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{ChannelHandlerContext => NettyChannelHandlerContext}
import org.infinispan.server.core.transport.{Channel, ChannelHandlerContext}

/**
 * A channel handler context adapter for Netty.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class ChannelHandlerContextAdapter(val ctx: NettyChannelHandlerContext) extends ChannelHandlerContext {
   
   override def getChannel: Channel = new ChannelAdapter(ctx.getChannel)

}