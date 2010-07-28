package org.infinispan.server.core.transport

/**
 * A protocol encoder. Normally, an encoder is used to write headers shared by all protocol operations or to delay
 * writing the protocol message so that all writing is done in a central place.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Encoder {

   /**
    * Transforms the specified message into another message and return the transformed message.
    * Note that you can not return {@code null}. You must return something, at least an empty channel buffer.
    */
   def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef
}