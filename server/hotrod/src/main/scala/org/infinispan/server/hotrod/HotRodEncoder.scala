package org.infinispan.server.hotrod

import org.infinispan.server.core.Logging
import org.infinispan.server.core.transport.{ChannelBuffer, ChannelHandlerContext, Channel, Encoder}
import OperationStatus._
import org.infinispan.server.core.transport.ChannelBuffers._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class HotRodEncoder extends Encoder {
   import HotRodEncoder._

   override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
      trace("Encode msg {0}", msg)
      val buffer: ChannelBuffer = msg match {
         case r: Response => writeHeader(r)
      }
      msg match {
         case s: StatsResponse => {
            buffer.writeUnsignedInt(s.stats.size)
            for ((key, value) <- s.stats) {
               buffer.writeString(key)
               buffer.writeString(value)
            }
         }
         case g: GetWithVersionResponse => {
            if (g.status == Success) {
               buffer.writeLong(g.version)
               buffer.writeRangedBytes(g.data.get)
            }
         }
         case g: GetResponse => if (g.status == Success) buffer.writeRangedBytes(g.data.get)
         case e: ErrorResponse => buffer.writeString(e.msg)
         case _ => if (buffer == null) throw new IllegalArgumentException("Response received is unknown: " + msg);         
      }
      buffer
   }

   private def writeHeader(r: Response): ChannelBuffer = {
      val buffer = dynamicBuffer
      buffer.writeByte(Magic.byteValue)
      buffer.writeUnsignedLong(r.messageId)
      buffer.writeByte(r.operation.id.byteValue)
      buffer.writeByte(r.status.id.byteValue)
      buffer.writeByte(0) // TODO: topology change marker, implemented later
      buffer
   }
   
}

object HotRodEncoder extends Logging {
   private val Magic = 0xA1
}