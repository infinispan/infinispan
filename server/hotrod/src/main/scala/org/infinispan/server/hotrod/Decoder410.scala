package org.infinispan.server.hotrod

import org.infinispan.server.core.UnknownCommandException
import org.infinispan.server.hotrod.OpCodes._
import org.infinispan.server.core.transport._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class Decoder410 {
   import Decoder410._

   def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer, id: Long): Command = {
      val op = getOpCode(buffer)
      val cacheName = buffer.readString
      val flags = Flags.toContextFlags(buffer.readUnsignedInt)
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
               new RetrievalCommand(cacheName, id, key, flags)({
                  (cache: Cache, command: RetrievalCommand) => cache.get(command)
               })
            }
            case _ => throw new UnknownCommandException("Command " + op + " not known")
         }
      command
   }

   private def getOpCode(buffer: ChannelBuffer): OpCodes.OpCode = {
      val op: Int = buffer.readUnsignedByte
      try {
         OpCodes.apply(op)
      } catch {
         case n: NoSuchElementException =>
            throw new UnknownCommandException("Operation code not valid: 0x" + op.toHexString + " (" + op + ")")
      }
   }

//   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
//      // no-op, handled by parent decoder
////      val t = e.getCause
////      error("Error", t)
////      ctx.sendDownstream(e)
////      val ch = ctx.getChannel
////      val buffers = ctx.getChannelBuffers
////      t match {
////         case u: UnknownCommandException =>
////      }
//
//
//   }

}

object Decoder410 extends Logging