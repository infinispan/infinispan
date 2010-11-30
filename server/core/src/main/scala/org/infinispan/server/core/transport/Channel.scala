package org.infinispan.server.core.transport

/**
 * A transport channel.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Channel {

   def write(message: Any): ChannelFuture

   def disconnect: ChannelFuture

   def getUnderlyingChannel: AnyRef

   def close: ChannelFuture

}