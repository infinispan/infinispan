package org.infinispan.server.core.transport

/**
 * A channel handler context.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class ChannelHandlerContext {
   def getChannel: Channel
}