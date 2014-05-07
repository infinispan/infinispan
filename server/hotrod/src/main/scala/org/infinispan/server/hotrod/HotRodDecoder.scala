package org.infinispan.server.hotrod

import org.infinispan.server.core._
import transport._
import OperationStatus._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.transport.ExtendedByteBuf._
import java.nio.channels.ClosedChannelException
import java.io.{IOException, StreamCorruptedException}
import java.lang.StringBuilder
import org.infinispan.container.entries.CacheEntry
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import javax.security.sasl.SaslServer
import org.infinispan.server.core.security.AuthorizingCallbackHandler
import org.infinispan.configuration.cache.Configuration
import org.infinispan.factories.ComponentRegistry

/**
 * Top level Hot Rod decoder that after figuring out the version, delegates the rest of the reading to the
 * corresponding versioned decoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodDecoder(cacheManager: EmbeddedCacheManager, transport: NettyTransport, server: HotRodServer)
        extends AbstractProtocolDecoder[Array[Byte], Array[Byte]](server.getConfiguration.authentication().enabled(), transport) with Constants {
   type SuitableHeader = HotRodHeader

   type SuitableParameters = RequestParameters
   private var isError = false

   private val isTrace = isTraceEnabled

   var saslServer : SaslServer = null
   var callbackHandler: AuthorizingCallbackHandler = null

   protected def createHeader: HotRodHeader = new HotRodHeader

   override def readHeader(buffer: ByteBuf, header: HotRodHeader): Option[Boolean] = {
      try {
         val magic = buffer.readUnsignedByte
         if (magic != MAGIC_REQ) {
            if (!isError) {
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic)
            } else {
               trace("Error happened previously, ignoring %d byte until we find the magic number again", magic)
               return None // Keep trying to read until we find magic
            }
         }
      } catch {
         case e: Exception => {
            isError = true
            throw e
         }
      }

      val messageId = readUnsignedLong(buffer)
      val version = buffer.readUnsignedByte.toByte

      try {
         val decoder = version match {
            case VERSION_10 | VERSION_11 | VERSION_12 | VERSION_13 => Decoder10
            case VERSION_20 => Decoder2x
            case _ => throw new UnknownVersionException(
               "Unknown version:" + version, version, messageId)
         }
         val endOfOp = decoder.readHeader(buffer, version, messageId, header)
         if (isTrace) trace("Decoded header %s", header)
         isError = false
         Some(endOfOp)
      } catch {
         case e: HotRodUnknownOperationException => {
            isError = true
            throw e
         }
         case e: Exception => {
            isError = true
            throw new RequestParsingException(
               "Unable to parse header", version, messageId, e)
         }
      }
   }

   override def getCache: Cache = {
      val cacheName = header.cacheName
      // Talking to the wrong cache are really request parsing errors
      // and hence should be treated as client errors
      if (cacheName.startsWith(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX))
         throw new RequestParsingException(
            "Remote requests are not allowed to topology cache. Do no send remote requests to cache '%s'".format(cacheName),
            header.version, header.messageId)

      var seenForFirstTime = false
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      if (server.isCacheNameKnown(cacheName)) {
         if (!(cacheManager.getCacheNames contains cacheName)) {
            isError = true // Mark it as error so that the rest of request is ignored
            throw new CacheNotFoundException(
               "Cache with name '%s' not found amongst the configured caches".format(cacheName),
               header.version, header.messageId)
         } else {
            seenForFirstTime = true
         }
      }

      val cache = server.getCacheInstance(cacheName, cacheManager, seenForFirstTime)
      header.decoder.getOptimizedCache(header, cache)
   }

   override def getCacheConfiguration: Configuration = {
      server.getCacheConfiguration(header.cacheName)
   }

   override def getCacheRegistry: ComponentRegistry = {
      server.getCacheRegistry(header.cacheName)
   }

   override def readKey(b: ByteBuf): (Array[Byte], Boolean) =
      header.decoder.readKey(header, b)

   override def readParameters(ch: Channel, b: ByteBuf): Boolean = {
      val (parameters, endOfOp) = header.decoder.readParameters(header, b)
      params = parameters
      endOfOp
   }

   override protected def readValue(b: ByteBuf) {
      b.readBytes(rawValue)
   }

   override def createValue(): Array[Byte] = rawValue

   override def createSuccessResponse(prev: Array[Byte]): AnyRef =
      header.decoder.createSuccessResponse(header, prev)

   override def createNotExecutedResponse(prev: Array[Byte]): AnyRef =
      header.decoder.createNotExecutedResponse(header, prev)

   override def createNotExistResponse: AnyRef =
      header.decoder.createNotExistResponse(header)

   override def createGetResponse(k: Array[Byte], entry: CacheEntry[Array[Byte], Array[Byte]]): AnyRef =
      header.decoder.createGetResponse(header, entry)

   override def createMultiGetResponse(pairs: Map[Array[Byte], CacheEntry[Array[Byte], Array[Byte]]]): AnyRef =
      null // Unsupported

   override protected def customDecodeHeader(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, header.decoder.customReadHeader(header, buffer, cache, server, ctx))

   override protected def customDecodeKey(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, header.decoder.customReadKey(header, buffer, cache, server, ctx.channel))

   override protected def customDecodeValue(ctx: ChannelHandlerContext, buffer: ByteBuf): AnyRef =
      writeResponse(ctx.channel, header.decoder.customReadValue(header, buffer, cache))

   override def createStatsResponse: AnyRef =
      header.decoder.createStatsResponse(header, cache.getStats, transport)

   override def createErrorResponse(t: Throwable): AnyRef = {
      t match {
         case h: HotRodException => h.response
         case c: ClosedChannelException => null
         case t: Throwable => {
            logErrorBeforeReadingRequest(t)
            new ErrorResponse(0, 0, "", 1, ServerError, 0, t.toString)
         }
      }
   }

   override protected def createServerException(e: Exception, b: ByteBuf): (HotRodException, Boolean) = {
      e match {
         case i: InvalidMagicIdException => {
            logExceptionReported(i)
            (new HotRodException(new ErrorResponse(
                  0, 0, "", 1, InvalidMagicOrMsgId, 0, i.toString), e), true)
         }
         case e: HotRodUnknownOperationException => {
            logExceptionReported(e)
            (new HotRodException(new ErrorResponse(
                  e.version, e.messageId, "", 1, UnknownOperation, 0, e.toString), e), true)
         }
         case u: UnknownVersionException => {
            logExceptionReported(u)
            (new HotRodException(new ErrorResponse(
                  u.version, u.messageId, "", 1, UnknownVersion, 0, u.toString), e), true)
         }
         case r: RequestParsingException => {
            logExceptionReported(r)
            val msg =
               if (r.getCause == null)
                  r.toString
               else
                  "%s: %s".format(r.getMessage, r.getCause.toString)
            (new HotRodException(new ErrorResponse(
                  r.version, r.messageId, "", 1, ParseError, 0, msg), e), true)
         }
         case i: IllegalStateException => {
            // Some internal server code could throw this, so make sure it's logged
            logExceptionReported(i)
            (new HotRodException(header.decoder.createErrorResponse(header, i), e), false)
         }
         case t: Throwable => (new HotRodException(header.decoder.createErrorResponse(header, t), e), false)
      }
   }

}

class UnknownVersionException(reason: String, val version: Byte, val messageId: Long)
        extends StreamCorruptedException(reason)

class HotRodUnknownOperationException(reason: String, val version: Byte, val messageId: Long)
        extends UnknownOperationException(reason)

class InvalidMagicIdException(reason: String) extends StreamCorruptedException(reason)

class RequestParsingException(reason: String, val version: Byte, val messageId: Long, cause: Exception)
        extends IOException(reason, cause) {
   def this(reason: String, version: Byte, messageId: Long) = this(reason, version, messageId, null)
}

class HotRodHeader extends RequestHeader {
   var version: Byte = _
   var messageId: Long = _
   var cacheName: String = _
   var flag: Int = _
   var clientIntel: Short = _
   var topologyId: Int = _
   var decoder: AbstractVersionedDecoder = _

   override def toString = {
      new StringBuilder().append("HotRodHeader").append("{")
         .append("op=").append(op)
         .append(", version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", cacheName=").append(cacheName)
         .append(", flag=").append(flag)
         .append(", clientIntelligence=").append(clientIntel)
         .append(", topologyId=").append(topologyId)
         .append("}").toString
   }
}

class CacheNotFoundException(msg: String, override val version: Byte, override val messageId: Long)
        extends RequestParsingException(msg, version, messageId)

class HotRodException(val response: ErrorResponse, cause: Throwable) extends Exception(cause)
