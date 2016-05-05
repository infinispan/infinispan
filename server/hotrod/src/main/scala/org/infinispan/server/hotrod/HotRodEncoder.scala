package org.infinispan.server.hotrod

import io.netty.buffer.ByteBuf
import io.netty.util.internal.PlatformDependent
import logging.Log
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.commons.util.Util
import io.netty.handler.codec.MessageToByteEncoder
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelHandler.Sharable
import org.infinispan.server.hotrod.Events.Event
import org.infinispan.server.hotrod.OperationStatus._

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Sharable
class HotRodEncoder(cacheManager: EmbeddedCacheManager, server: HotRodServer)
        extends MessageToByteEncoder[Any](PlatformDependent.directBufferPreferred()) with Constants with Log {

   private lazy val isClustered: Boolean = cacheManager.getCacheManagerConfiguration.transport.transport != null
   private lazy val addressCache: AddressCache =
      if (isClustered) cacheManager.getCache(server.getConfiguration.topologyCacheName) else null
   private val isTrace = isTraceEnabled

   def encode(ctx: ChannelHandlerContext, msg: Any, buf: ByteBuf): Unit = {
      try {
         trace("Encode msg %s", msg)

         msg match {
            case r: Response =>
               val encoder = getEncoder(r.version)
               try {
                  r.version match {
                     case ver if Constants.isVersionKnown(ver) =>
                        encoder.writeHeader(r, buf, addressCache, server)
                     // if error before reading version, don't send any topology changes
                     // cos the encoding might vary from one version to the other
                     case 0 => encoder.writeHeader(r, buf, null, server)
                  }

                  encoder.writeResponse(r, buf, cacheManager, server)
               }
               catch {
                  case t: Throwable =>
                     logErrorWritingResponse(r.messageId, t)
                     buf.clear() // reset buffer
                  val error = new ErrorResponse(r.version, r.messageId, r.cacheName, r.clientIntel, ServerError, r.topologyId, t.toString)
                     encoder.writeHeader(error, buf, addressCache, server)
                     encoder.writeResponse(error, buf, cacheManager, server)
               }
            case e: Event =>
               val encoder = getEncoder(e.version)
               encoder.writeEvent(e, buf)
            case None =>
               // Do nothing
            case _ =>
               logErrorUnexpectedMessage(msg)
         }

         if (isTrace)
            trace("Write buffer contents %s to channel %s",
               Util.hexDump(buf.nioBuffer), ctx.channel)
      }
      catch {
         case t: Throwable =>
            logErrorEncodingMessage(msg, t)
            throw t
      }
   }

   private def getEncoder(version: Byte): AbstractVersionedEncoder = {
      version match {
         case ver if Constants.isVersion2x(ver) => Encoder2x
         case ver if Constants.isVersion13(ver) => Encoders.Encoder13
         case ver if Constants.isVersion12(ver) => Encoders.Encoder12
         case ver if Constants.isVersion11(ver) => Encoders.Encoder11
         case ver if Constants.isVersion10(ver) => Encoders.Encoder10
         case 0 => Encoder2x
      }
   }

}
