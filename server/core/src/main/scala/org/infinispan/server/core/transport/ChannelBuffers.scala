package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class ChannelBuffers {
//   def wrappedBuffer(buffers: ChannelBuffer*): ChannelBuffer
//   def wrappedBuffer(buffer: ChannelBuffer): ChannelBuffer
   def wrappedBuffer(array: Array[Byte]*): ChannelBuffer
   def dynamicBuffer(): ChannelBuffer
}