package org.infinispan.server.hotrod

import org.infinispan.server.core.UnknownCommandException
import org.infinispan.server.hotrod.OpCodes._
import org.infinispan.server.core.transport._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class Decoder410 extends NoStateDecoder {
   import Decoder410._

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): Command = {
      val op = OpCodes.apply(buffer.readUnsignedByte)
      val cacheName = buffer.readString
      val id = buffer.readUnsignedLong
      val flags = Flags.extract(buffer.readUnsignedInt)
      val command: Command =
         op match {                                   
            case PutRequest => {
               val key = buffer.readRangedBytes
               val lifespan = buffer.readUnsignedInt
               val maxIdle = buffer.readUnsignedInt
               val value = buffer.readRangedBytes
               new StorageCommand(cacheName, id, key, lifespan, maxIdle, value, flags)({
                  (cache: Cache, command: StorageCommand) => cache.put(command)
               })
            }
            case GetRequest => {
               val key = buffer.readRangedBytes
               new RetrievalCommand(cacheName, id, key)({
                  (cache: Cache, command: RetrievalCommand) => cache.get(command)
               })
            }
            case _ => throw new UnknownCommandException("Command " + op + " not known")
         }
      command
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }

}

object Decoder410 extends Logging