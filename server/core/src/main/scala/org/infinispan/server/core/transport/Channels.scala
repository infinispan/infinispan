package org.infinispan.server.core.transport

import netty.ChannelsAdapter

/**
 * Abstraction for executing internal channel operations
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
object Channels {

   def fireExceptionCaught(ch: Channel, cause: Throwable) {
      ChannelsAdapter.fireExceptionCaught(ch, cause)
   }

}