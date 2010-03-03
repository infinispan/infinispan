package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{ChannelBuffer, ChannelHandlerContext, Channel, Encoder}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class Encoder410 extends Encoder {
   import Encoder410._

   private val Magic = 0xA1

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Object) = {
      val buffer: ChannelBuffer =
         msg match {
            case r: Response => {
               val buff = ctx.getChannelBuffers.dynamicBuffer
               buff.writeByte(Magic.byteValue)
               buff.writeByte(r.opCode.id.byteValue)
               VLong.write(buff, r.id)
               buff.writeByte(r.status.id.byteValue)
               buff
            }
      }
      buffer
   }
   
}

object Encoder410 extends Logging