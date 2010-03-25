package org.infinispan.server.hotrod

import org.infinispan.Cache
import org.infinispan.stats.Stats
import java.io.StreamCorruptedException
import org.infinispan.server.core._
import OperationStatus._
import HotRodOperation._
import ProtocolFlag._
import transport._
import org.infinispan.manager.{DefaultCacheManager, CacheManager}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class HotRodDecoder(cacheManager: CacheManager) extends AbstractProtocolDecoder[CacheKey, CacheValue] {
   import HotRodDecoder._
   
   type SuitableHeader = HotRodHeader
   type SuitableParameters = RequestParameters

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

   override def readKey(header: HotRodHeader, buffer: ChannelBuffer): CacheKey = header.decoder.readKey(buffer)

   override def readKeys(header: HotRodHeader, buffer: ChannelBuffer): Array[CacheKey] =
      header.decoder.readKeys(buffer)

   override def readParameters(header: HotRodHeader, buffer: ChannelBuffer): Option[RequestParameters] =
      header.decoder.readParameters(header, buffer)

   override def createValue(header: HotRodHeader, params: RequestParameters, nextVersion: Long): CacheValue =
      header.decoder.createValue(params, nextVersion)

   override def sendPutResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers): AnyRef =
      header.decoder.sendPutResponse(header.messageId)

   override def sendGetResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers,
                                k: CacheKey, v: CacheValue): AnyRef =
      header.decoder.sendGetResponse(header.messageId, v, header.op)

   override def sendPutIfAbsentResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers,
                                        prev: CacheValue): AnyRef =
      header.decoder.sendPutIfAbsentResponse(header.messageId, prev)

   override def sendReplaceResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers,
                                    prev: CacheValue): AnyRef =
      header.decoder.sendReplaceResponse(header.messageId, prev)

   override def sendReplaceIfUnmodifiedResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers,
                                                v: Option[CacheValue], prev: Option[CacheValue]): AnyRef =
      header.decoder.sendReplaceIfUnmodifiedResponse(header.messageId, v, prev)

   override def sendRemoveResponse(header: HotRodHeader, ch: Channel, buffers: ChannelBuffers,
                                   prev: CacheValue): AnyRef =
      header.decoder.sendRemoveResponse(header.messageId, prev)

   override def sendMultiGetResponse(header: HotRodHeader, ctx: ChannelHandlerContext,
                                     pairs: Map[CacheKey, CacheValue]): AnyRef = null // Unsupported

   override def handleCustomRequest(header: HotRodHeader, ctx: ChannelHandlerContext,
                                    buffer: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef =
      header.decoder.handleCustomRequest(header, buffer, cache)

   override def sendResponse(header: HotRodHeader, ctx: ChannelHandlerContext, stats: Stats): AnyRef =
      header.decoder.sendStatsResponse(header, stats)

   override def sendResponse(ctx: ChannelHandlerContext, t: Throwable): AnyRef = {
      val ch = ctx.getChannel
      val buffers = ctx.getChannelBuffers
      val errorResponse = t match {
         case se: ServerException => {
            val messageId = se.header.asInstanceOf[HotRodHeader].messageId
            se.getCause match {
               case imie: InvalidMagicIdException => new ErrorResponse(0, InvalidMagicOrMsgId, imie.toString)
               case uoe: UnknownOperationException => new ErrorResponse(messageId, UnknownOperation, uoe.toString)
               case uve: UnknownVersionException => new ErrorResponse(messageId, UnknownVersion, uve.toString)
               // TODO add more cases
               case e: Exception => new ErrorResponse(messageId, ServerError, e.toString)  
            }
         }
      }
      ch.write(errorResponse)
      null
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