package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{Channel => NettyChannel}
import org.infinispan.server.core.transport.{ChannelBuffer, ChannelFuture, Channel}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class ChannelAdapter(val ch: NettyChannel) extends Channel {

   override def disconnect: ChannelFuture = new ChannelFutureAdapter(ch.disconnect());

   override def write(message: Any): ChannelFuture = {
      val toWrite = message match {
         case buffer: ChannelBuffer => buffer.getUnderlyingChannelBuffer
         case _ => message
      }
      new ChannelFutureAdapter(ch.write(toWrite));
   }

}