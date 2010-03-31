package org.infinispan.server.core.transport

import netty.ChannelBuffersAdapter

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
object ChannelBuffers {
   
   def wrappedBuffer(array: Array[Byte]*): ChannelBuffer = {
      ChannelBuffersAdapter.wrappedBuffer(array : _*)
   }

   def dynamicBuffer(): ChannelBuffer = {
      ChannelBuffersAdapter.dynamicBuffer
   }

}