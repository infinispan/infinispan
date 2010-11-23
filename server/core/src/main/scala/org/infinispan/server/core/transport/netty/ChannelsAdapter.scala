package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel.{Channel => NettyChannel}
import org.jboss.netty.channel.{Channels => NettyChannels}
import org.infinispan.server.core.transport.{Channel}

/**
 * Adapter class for Netty's Channels class.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
object ChannelsAdapter {

   def fireExceptionCaught(ch: Channel, cause: Throwable) {
      NettyChannels.fireExceptionCaught(ch.getUnderlyingChannel.asInstanceOf[NettyChannel], cause)
   }

}