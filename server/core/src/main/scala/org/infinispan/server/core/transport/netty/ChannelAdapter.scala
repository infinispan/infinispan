package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{Channel => NettyChannel}
import org.infinispan.server.core.transport.{ChannelBuffer, ChannelFuture, Channel}

/**
 * Transport channel implementation for Netty transport.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class ChannelAdapter(ch: NettyChannel) extends Channel {

   override def disconnect: ChannelFuture = new ChannelFutureAdapter(ch.disconnect())

   override def write(message: Any): ChannelFuture = {
      val toWrite = message match {
         case buffer: ChannelBuffer => buffer.getUnderlyingChannelBuffer
         case _ => message
      }
      new ChannelFutureAdapter(ch.write(toWrite))
   }

   override def getUnderlyingChannel: AnyRef = ch

   override def close: ChannelFuture = new ChannelFutureAdapter(ch.close)
}