package org.infinispan.server.hotrod

import org.infinispan.stats.Stats
import org.infinispan.server.core._
import transport._
import OperationStatus._
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import java.io.StreamCorruptedException
import org.infinispan.server.hotrod.ProtocolFlag._
import org.infinispan.server.hotrod.OperationResponse._
import java.nio.channels.ClosedChannelException
import org.infinispan.{CacheException, Cache}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodDecoder(cacheManager: CacheManager) extends AbstractProtocolDecoder[CacheKey, CacheValue] {
   import HotRodDecoder._
   
   type SuitableHeader = HotRodHeader
   type SuitableParameters = RequestParameters

   private var isError = false
   private var joined = false

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
      // TODO: Document DefaultCacheManager.DEFAULT_CACHE_NAME usage in wiki
      val cacheName = header.cacheName
      if (cacheName != DefaultCacheManager.DEFAULT_CACHE_NAME && !cacheManager.getCacheNames.contains(cacheName))
         throw new CacheNotFoundException("Cache with name '" + cacheName + "' not found amongst the configured caches")
      
      if (cacheName == DefaultCacheManager.DEFAULT_CACHE_NAME) cacheManager.getCache[CacheKey, CacheValue]
      else cacheManager.getCache(cacheName)
   }

   override def readKey(h: HotRodHeader, b: ChannelBuffer): CacheKey =
      h.decoder.readKey(b)

   override def readKeys(h: HotRodHeader, b: ChannelBuffer): Array[CacheKey] =
      h.decoder.readKeys(b)

   override def readParameters(h: HotRodHeader, b: ChannelBuffer): Option[RequestParameters] =
      h.decoder.readParameters(h, b)

   override def createValue(h: HotRodHeader, p: RequestParameters, nextVersion: Long): CacheValue =
      h.decoder.createValue(p, nextVersion)

   override def createSuccessResponse(h: HotRodHeader, p: Option[RequestParameters], prev: CacheValue): AnyRef =
      h.decoder.createSuccessResponse(h, prev)

   override def createNotExecutedResponse(h: HotRodHeader, p: Option[RequestParameters], prev: CacheValue): AnyRef =
      h.decoder.createNotExecutedResponse(h, prev)

   override def createNotExistResponse(h: HotRodHeader, p: Option[RequestParameters]): AnyRef =
      h.decoder.createNotExistResponse(h)

   override def createGetResponse(h: HotRodHeader, k: CacheKey, v: CacheValue): AnyRef =
      h.decoder.createGetResponse(h, v, h.op)

   override def createMultiGetResponse(h: HotRodHeader, pairs: Map[CacheKey, CacheValue]): AnyRef =
      null // Unsupported

   override def handleCustomRequest(h: HotRodHeader, b: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef =
      h.decoder.handleCustomRequest(h, b, cache)

   override def createStatsResponse(h: HotRodHeader, stats: Stats): AnyRef =
      h.decoder.createStatsResponse(h, stats)

   override def createErrorResponse(t: Throwable): AnyRef = {
      t match {
         case se: ServerException => {
            val h = se.header.asInstanceOf[HotRodHeader]
            se.getCause match {
               case i: InvalidMagicIdException => new ErrorResponse(0, "", 1, InvalidMagicOrMsgId, None, i.toString)
               case u: UnknownOperationException => new ErrorResponse(h.messageId, "", 1, UnknownOperation, None, u.toString)
               case u: UnknownVersionException => new ErrorResponse(h.messageId, "", 1, UnknownVersion, None, u.toString)
               case t: Throwable => h.decoder.createErrorResponse(h, t)
            }
         }
         case c: ClosedChannelException => null
         case t: Throwable => new ErrorResponse(0, "", 1, ServerError, None, t.toString)
      }
   }

}

object HotRodDecoder extends Logging {
   private val Magic = 0xA0
   private val Version10 = 10
}

class UnknownVersionException(reason: String) extends StreamCorruptedException(reason)

class InvalidMagicIdException(reason: String) extends StreamCorruptedException(reason)

class HotRodHeader(override val op: Enumeration#Value, val messageId: Long, val cacheName: String,
                   val flag: ProtocolFlag, val clientIntel: Short, val topologyId: Int,
                   val decoder: AbstractVersionedDecoder) extends RequestHeader(op) {
   override def toString = {
      new StringBuilder().append("HotRodHeader").append("{")
         .append("op=").append(op)
         .append(", messageId=").append(messageId)
         .append(", cacheName=").append(cacheName)
         .append(", flag=").append(flag)
         .append(", clientIntelligence=").append(clientIntel)
         .append(", topologyId=").append(topologyId)
         .append("}").toString
   }
}

class ErrorHeader(override val messageId: Long) extends HotRodHeader(ErrorResponse, messageId, "", NoFlag, 0, 0, null) {
   override def toString = {
      new StringBuilder().append("ErrorHeader").append("{")
         .append("messageId=").append(messageId)
         .append("}").toString
   }
}

class CacheNotFoundException(msg: String) extends CacheException(msg)