package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.ChannelHandler
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{ChannelHandlerContext => NettyChannelHandlerContext}
import org.jboss.netty.channel.{Channel => NettyChannel}
import org.infinispan.server.core.transport.{ChannelBuffer, Encoder}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@ChannelHandler.Sharable
class EncoderAdapter(encoder: Encoder) extends OneToOneEncoder {

   protected override def encode(nCtx: NettyChannelHandlerContext, ch: NettyChannel, msg: AnyRef): AnyRef = {
      var ret = encoder.encode(new ChannelHandlerContextAdapter(nCtx), new ChannelAdapter(ch), msg);
      ret = ret match {
         case cb: ChannelBuffer => cb.getUnderlyingChannelBuffer
         case _ => ret
      }
      ret
   }
   
}