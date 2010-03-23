package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class Channel {
   def write(message: Any): ChannelFuture
   def disconnect: ChannelFuture
}