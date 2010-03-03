package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{ExceptionEvent, ChannelHandlerContext, ChannelBuffer, Decoder}
import org.infinispan.server.core.UnknownCommandException

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class Decoder410 extends Decoder[NoState] {
   import Decoder410._

   val Put = 0x01

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer, state: NoState): StorageCommand = {
      val op = buffer.readUnsignedByte
      val cacheName = readString(buffer)
      val id = buffer.readUnsignedLong
      val flags = Flags.extractFlags(buffer.readUnsignedInt)
      val command: StorageCommand =
         op match {
            case Put => {
               val key = readByteArray(buffer)
               val lifespan = buffer.readUnsignedInt
               val maxIdle = buffer.readUnsignedInt
               val value = readByteArray(buffer)
               new StorageCommand(cacheName, id, key, lifespan, maxIdle, value, flags)({
                  (cache: Cache, command: StorageCommand) => cache.put(command)
               })
            }
            case _ => throw new UnknownCommandException("Command " + op + " not known")
         }
      command
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }

   private def readString(buffer: ChannelBuffer): String = {
      val array = new Array[Byte](buffer.readUnsignedInt)
      buffer.readBytes(array)
      new String(array, "UTF8")
   }

   private def readByteArray(buffer: ChannelBuffer): Array[Byte] = {
      val array = new Array[Byte](buffer.readUnsignedInt)
      buffer.readBytes(array)
      array
   }

}

object Decoder410 extends Logging