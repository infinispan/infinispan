package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class Encoder {
   def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef
}