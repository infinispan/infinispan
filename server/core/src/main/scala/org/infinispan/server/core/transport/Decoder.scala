package org.infinispan.server.core.transport

/**
 * A protocol decoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Decoder {

   /**
    * Decodes the received packets so far into a frame.
    */
   def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): AnyRef

   /**
    * Invoked when an exception was raised by an I/O thread or a channel handler.
    */
   def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent)

   /**
    * Decodes the received data so far into a frame when the channel is disconnected.
    */
   def decodeLast(ctx: ChannelHandlerContext, buffer: ChannelBuffer): AnyRef
}