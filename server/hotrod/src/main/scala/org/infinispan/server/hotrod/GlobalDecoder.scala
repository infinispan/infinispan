package org.infinispan.server.hotrod

import java.io.StreamCorruptedException
import org.infinispan.server.core.transport._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class GlobalDecoder extends NoStateDecoder {
   import GlobalDecoder._

   private val Magic = 0xA0
   private val Version410 = 41

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): Object = {
      val magic = buffer.readUnsignedByte()
      if (magic != Magic) {
         throw new StreamCorruptedException("Magic byte incorrect: " + magic)
      }

      val version = buffer.readUnsignedByte()
      val decoder =
         version match {
            case Version410 => new Decoder410
            case _ => throw new StreamCorruptedException("Unknown version:" + version)
         }
      val command = decoder.decode(ctx, buffer)
      trace("Decoded msg {0}", command)
      command
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }

}

object GlobalDecoder extends Logging