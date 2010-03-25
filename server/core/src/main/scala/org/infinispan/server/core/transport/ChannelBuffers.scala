package org.infinispan.server.core.transport

import netty.ChannelBuffersAdapter

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object ChannelBuffers {
   
   def wrappedBuffer(array: Array[Byte]*): ChannelBuffer = {
      ChannelBuffersAdapter.wrappedBuffer(array : _*)
   }

   def dynamicBuffer(): ChannelBuffer = {
      ChannelBuffersAdapter.dynamicBuffer
   }

}