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
         case r: ResponseWithPrevious => {
            if (r.previous == None)
               buffer.writeUnsignedInt(0)
            else
               buffer.writeRangedBytes(r.previous.get)
         }
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
      if (r.topologyResponse != None) {
         buffer.writeByte(1) // Topology changed
         r.topologyResponse.get match {
            case t: TopologyAwareResponse => {
               buffer.writeUnsignedInt(t.view.topologyId)
               buffer.writeUnsignedInt(t.view.members.size)
               t.view.members.foreach{address =>
                  buffer.writeString(address.host)
                  buffer.writeUnsignedShort(address.port)
               }
            }
            case h: HashDistAwareResponse => {
               // TODO: Implement reply to hash dist responses
            }
         }
      } else {
         buffer.writeByte(0) // No topology change
      }
      buffer
   }
   
}

object HotRodEncoder extends Logging {
   private val Magic = 0xA1
}