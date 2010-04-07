package org.infinispan.server.core.transport.netty

import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.jboss.netty.channel.{ExceptionEvent => NettyExceptionEvent, ChannelHandlerContext => NettyChannelHandlerContext, Channel => NettyChannel}
import org.jboss.netty.buffer.{ChannelBuffer => NettyChannelBuffer}
import org.infinispan.server.core.transport._
import org.infinispan.server.core.Logging;

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class DecoderAdapter(decoder: Decoder) extends ReplayingDecoder[NoState](true) {

   override def decode(nCtx: NettyChannelHandlerContext, channel: NettyChannel,
                       nBuffer: NettyChannelBuffer, passedState: NoState): AnyRef = {
      val ctx = new ChannelHandlerContextAdapter(nCtx);
      val buffer = new ChannelBufferAdapter(nBuffer);
      decoder.decode(ctx, buffer);
   }

   override def exceptionCaught(ctx: NettyChannelHandlerContext, e: NettyExceptionEvent) {
      decoder.exceptionCaught(new ChannelHandlerContextAdapter(ctx), new ExceptionEventAdapter(e));
   }

}