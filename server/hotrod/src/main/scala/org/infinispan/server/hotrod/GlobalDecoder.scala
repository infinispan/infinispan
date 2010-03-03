package org.infinispan.server.hotrod

import java.io.StreamCorruptedException
import org.infinispan.server.core.transport.{ExceptionEvent, Decoder, ChannelBuffer, ChannelHandlerContext}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */
class GlobalDecoder extends Decoder[NoState] {
   import GlobalDecoder._

   private val Magic = 0xA0
   private val Version410 = 41

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer, state: NoState): Object = {
//      trace("Buffer contains: {0}", buffer)
//      state match {
//         case NoState.VOID => {
//      val header = buffer.readBytes(2)
      val magic = buffer.readUnsignedByte()
      if (magic != Magic) {
         buffer.resetReaderIndex()
         throw new StreamCorruptedException("Magic byte incorrect: " + magic)
      }

      val version = buffer.readUnsignedByte()
      val decoder =
         version match {
            case Version410 => new Decoder410
            case _ => throw new StreamCorruptedException("Unknown version:" + version)
         }
      decoder.decode(ctx, buffer, state)
//         }
//      }
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }

//   private def getHeader(buffer: ChannelBuffer): ChannelBuffer = {
//      if (buffer.readableBytes() < 2) null
//
//   }
//
//   private def verifyMagic(buffer: ChannelBuffer) {
//   }
//
//   private def getVersion(buffer: ChannelBuffer) = {
//
//   }

}

object GlobalDecoder extends Logging