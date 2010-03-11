package org.infinispan.server.hotrod

import java.io.StreamCorruptedException
import org.infinispan.server.core.transport._
import org.infinispan.server.core.UnknownCommandException
import org.infinispan.util.concurrent.TimeoutException

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class GlobalDecoder extends NoStateDecoder {

   import GlobalDecoder._

   private val Magic = 0xA0
   private val Version410 = 41
   @volatile private var isError = false

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): Object = {
      val magic = buffer.readUnsignedByte()
      if (magic != Magic) {
         if (!isError) {
            val t = new StreamCorruptedException("Error reading magic byte or message id: " + magic)
            return createErrorResponse(0, Status.InvalidMagicOrMsgId, t) 
         } else {
            trace("Error happened previously, ignoring {0} byte until we find the magic number again", magic)
            return null // Keep trying to read until we find magic
         }
      }

      val id = buffer.readUnsignedLong

      try {
         val version = buffer.readUnsignedByte()
         val decoder =
            version match {
               case Version410 => new Decoder410
               case _ => {
                  isError = true
                  return new ErrorResponse(id, OpCodes.ErrorResponse, Status.UnknownVersion, "Unknown version:" + version)
               }
            }
         val command = decoder.decode(ctx, buffer, id)
         trace("Decoded msg {0}", command)
         isError = false
         command
      } catch {
         case u: UnknownCommandException => createErrorResponse(id, Status.UnknownCommand, u)
         case s: StreamCorruptedException => createErrorResponse(id, Status.ParseError, s)
         case o: TimeoutException => createErrorResponse(id, Status.CommandTimedOut, o)
         case t: Throwable => createErrorResponse(id, Status.ServerError, t)
      }
   }

   private def createErrorResponse(id: Long, status: Status.Status, t: Throwable): ErrorResponse = {
      isError = true
      error("Error processing command", t)
      val m = if (t.getMessage != null) t.getMessage else ""
      new ErrorResponse(id, OpCodes.ErrorResponse, status, m)
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Unexpected error", e.getCause)
   }

}

object GlobalDecoder extends Logging