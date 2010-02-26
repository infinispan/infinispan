package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{ExceptionEvent, ChannelHandlerContext, ChannelBuffer, Decoder}
import org.infinispan.server.core.UnknownCommandException

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

class Decoder410 extends Decoder[NoState] {
   import Decoder410._

   val Put = 0x01

   @Override
   def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer, state: NoState): StorageCommand = {
      val op = buffer.readUnsignedByte
      val cacheName = readString(buffer)
      val id = VLong.read(buffer)
      val flags = Flags.extractFlags(VInt.read(buffer))
      val command: StorageCommand =
         op match {
            case Put => {
               val key = readByteArray(buffer)
               val lifespan = VInt.read(buffer)
               val maxIdle = VInt.read(buffer)
               val value = readByteArray(buffer)
               new StorageCommand(cacheName, key, lifespan, maxIdle, value, flags)({
                  (cache: Cache, command: StorageCommand) => cache.put(command)
               })
            }
            case _ => throw new UnknownCommandException("Command " + op + " not known")
         }
      command
   }

   @Override
   def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }

   private def readString(buffer: ChannelBuffer): String = {
      val array = new Array[Byte](VInt.read(buffer))
      buffer.readBytes(array)
      new String(array, "UTF8")
   }

   private def readByteArray(buffer: ChannelBuffer): Array[Byte] = {
      val array = new Array[Byte](VInt.read(buffer))
      buffer.readBytes(array)
      array
   }

}

object Decoder410 extends Logging