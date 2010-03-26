package org.infinispan.server.hotrod

import org.infinispan.Cache
import org.infinispan.stats.Stats
import org.infinispan.server.core._
import transport._
import OperationStatus._
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import java.io.{IOException, StreamCorruptedException}
import org.infinispan.util.concurrent.TimeoutException

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class HotRodDecoder(cacheManager: CacheManager) extends AbstractProtocolDecoder[CacheKey, CacheValue] {
   import HotRodDecoder._
   
   type SuitableHeader = HotRodHeader
   type SuitableParameters = RequestParameters

   // TODO: Ask trustin whether this needs to be a volatile or not, depends on how decoders are shared
   @volatile private var isError = false

   override def readHeader(buffer: ChannelBuffer): HotRodHeader = {
      try {
         val magic = buffer.readUnsignedByte
         if (magic != Magic) {
            if (!isError) {               
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic)
            } else {
               trace("Error happened previously, ignoring {0} byte until we find the magic number again", magic)
               return null // Keep trying to read until we find magic
            }
         }
      } catch {
         case e: Exception => {
            isError = true
            throw new ServerException(new ErrorHeader(0), e)
         }
      }

      val messageId = buffer.readUnsignedLong
      
      try {
         val version = buffer.readUnsignedByte
         val decoder = version match {
            case Version10 => new Decoder10(cacheManager)
            case _ => throw new UnknownVersionException("Unknown version:" + version)
         }
         val header = decoder.readHeader(buffer, messageId)
         trace("Decoded header {0}", header)
         isError = false
         header
      } catch {
         case e: Exception => {
            isError = true
            throw new ServerException(new ErrorHeader(messageId), e)
         }
      }
   }

   override def getCache(header: HotRodHeader): Cache[CacheKey, CacheValue] = {
      if (header.cacheName == DefaultCacheManager.DEFAULT_CACHE_NAME) cacheManager.getCache[CacheKey, CacheValue]
      else cacheManager.getCache(header.cacheName)
   }

   override def readKey(h: HotRodHeader, b: ChannelBuffer): CacheKey =
      h.decoder.readKey(b)

   override def readKeys(h: HotRodHeader, b: ChannelBuffer): Array[CacheKey] =
      h.decoder.readKeys(b)

   override def readParameters(h: HotRodHeader, b: ChannelBuffer): Option[RequestParameters] =
      h.decoder.readParameters(h, b)

   override def createValue(h: HotRodHeader, p: RequestParameters, nextVersion: Long): CacheValue =
      h.decoder.createValue(p, nextVersion)

   override def createSuccessResponse(h: HotRodHeader, p: Option[RequestParameters]): AnyRef =
      h.decoder.createSuccessResponse(h)

   override def createNotExecutedResponse(h: HotRodHeader, p: Option[RequestParameters]): AnyRef =
      h.decoder.createNotExecutedResponse(h)

   override def createNotExistResponse(h: HotRodHeader, p: Option[RequestParameters]): AnyRef =
      h.decoder.createNotExistResponse(h)

   override def createGetResponse(h: HotRodHeader, k: CacheKey, v: CacheValue): AnyRef =
      h.decoder.createGetResponse(h.messageId, v, h.op)

   override def createMultiGetResponse(h: HotRodHeader, pairs: Map[CacheKey, CacheValue]): AnyRef =
      null // Unsupported

   override def handleCustomRequest(h: HotRodHeader, b: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef =
      h.decoder.handleCustomRequest(h, b, cache)

   override def createStatsResponse(h: HotRodHeader, stats: Stats): AnyRef =
      h.decoder.createStatsResponse(h, stats)

   override def createErrorResponse(t: Throwable): AnyRef = {
      t match {
         case se: ServerException => {
            val messageId = se.header.asInstanceOf[HotRodHeader].messageId
            se.getCause match {
               case i: InvalidMagicIdException => new ErrorResponse(0, InvalidMagicOrMsgId, i.toString)
               case u: UnknownOperationException => new ErrorResponse(messageId, UnknownOperation, u.toString)
               case u: UnknownVersionException => new ErrorResponse(messageId, UnknownVersion, u.toString)
               case i: IOException => new ErrorResponse(messageId, ParseError, i.toString)
               case t: TimeoutException => new ErrorResponse(messageId, OperationTimedOut, t.toString)
               case e: Exception => new ErrorResponse(messageId, ServerError, e.toString)
            }
         }
      }
   }

   override def start {}

   override def stop {}
}

object HotRodDecoder extends Logging {
   private val Magic = 0xA0
   private val Version10 = 10
}

class UnknownVersionException(reason: String) extends StreamCorruptedException(reason)

class InvalidMagicIdException(reason: String) extends StreamCorruptedException(reason)