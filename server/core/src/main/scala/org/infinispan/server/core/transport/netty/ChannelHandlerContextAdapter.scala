package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{ChannelHandlerContext => NettyChannelHandlerContext}
import org.infinispan.server.core.transport.{ChannelBuffers, Channel, ChannelHandlerContext}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class ChannelHandlerContextAdapter(val ctx: NettyChannelHandlerContext) extends ChannelHandlerContext {
   
   override def getChannel: Channel = new ChannelAdapter(ctx.getChannel)

   // TODO: Remove this from here and make it available via an object, this would clean up unnecessary params 
   override def getChannelBuffers: ChannelBuffers = ChannelBuffersAdapter
   
}