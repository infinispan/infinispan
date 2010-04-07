package org.infinispan.server.core.transport.netty

import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.jboss.netty.channel.{ExceptionEvent => NettyExceptionEvent, ChannelHandlerContext => NettyChannelHandlerContext, Channel => NettyChannel}
import org.jboss.netty.buffer.{ChannelBuffer => NettyChannelBuffer}
import org.infinispan.server.core.transport._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class DecoderAdapter(decoder: Decoder) extends ReplayingDecoder[NoState](true) {

   override def decode(nCtx: NettyChannelHandlerContext, channel: NettyChannel,
                       nBuffer: NettyChannelBuffer, passedState: NoState): AnyRef = {
      decoder.decode(new ChannelHandlerContextAdapter(nCtx), new ChannelBufferAdapter(nBuffer))
   }

   override def exceptionCaught(ctx: NettyChannelHandlerContext, e: NettyExceptionEvent) {
      decoder.exceptionCaught(new ChannelHandlerContextAdapter(ctx), new ExceptionEventAdapter(e));
   }

   override def decodeLast(nCtx: NettyChannelHandlerContext, channel: NettyChannel,
                  nBuffer: NettyChannelBuffer, passedState: NoState): AnyRef = {
      decoder.decodeLast(new ChannelHandlerContextAdapter(nCtx), new ChannelBufferAdapter(nBuffer));
   }

}