package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Channel {
   def write(message: Any): ChannelFuture
   def disconnect: ChannelFuture
}