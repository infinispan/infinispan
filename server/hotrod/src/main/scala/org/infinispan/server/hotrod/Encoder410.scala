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
      trace("Encode msg {0}", msg)
//      try {
         val buffer: ChannelBuffer =
            msg match {
               case r: Response => {
                  val buffer = ctx.getChannelBuffers.dynamicBuffer
                  buffer.writeByte(Magic.byteValue)
                  buffer.writeUnsignedLong(r.id)
                  buffer.writeByte(r.opCode.id.byteValue)
                  buffer.writeByte(r.status.id.byteValue)
                  buffer
               }
         }
         msg match {
            case rr: RetrievalResponse => if (rr.status == Status.Success) buffer.writeRangedBytes(rr.value)
            case er: ErrorResponse => buffer.writeString(er.msg)
            case _ => {
               if (buffer == null)
                  throw new IllegalArgumentException("Response received is unknown: " + msg);
            }
         }
         buffer
//      } catch {
//         case t: Throwable => {
//            val buffer = ctx.getChannelBuffers.dynamicBuffer
//            buffer.writeByte(Magic.byteValue)
//         }
//      }
   }
   
}

object Encoder410 extends Logging