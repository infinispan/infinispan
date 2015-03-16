package org.infinispan.server.hotrod

import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.TimeUnit

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.DecoderException
import io.netty.util.CharsetUtil
import org.infinispan.AdvancedCache
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.{EntryVersion, NumericVersion, NumericVersionGenerator, VersionGenerator}
import org.infinispan.context.Flag
import org.infinispan.factories.ComponentRegistry
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.{EmbeddedMetadata, Metadata}
import org.infinispan.remoting.rpc.RpcManager
import org.infinispan.server.core.ServerConstants
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.server.hotrod.logging.Log

/**
 * Invokes operations against the cache based on the state kept during decoding process
 *
 */
class CacheDecodeContext(server: HotRodServer) extends ServerConstants with Log {

   type BytesResponse = Bytes => Response

   val isTrace = isTraceEnabled
   val SecondsInAMonth = 60 * 60 * 24 * 30

   var isError = false
   var decoder: AbstractVersionedDecoder = _
   var header: HotRodHeader = _
   var cache: AdvancedCache[Bytes, Bytes] = _
   var defaultLifespanTime: Long = _
   var defaultMaxIdleTime: Long = _
   var key: Bytes = _
   var rawValue: Bytes = _
   var params: RequestParameters = _

   def resetParams(): Unit = {
      params = null
      rawValue = null
   }

   def createErrorResponse(t: Throwable): AnyRef = {
      t match {
         case d: DecoderException => d.getCause match {
            case h: HotRodException => h.response
            case _ => createErrorResponseBeforeReadingRequest(t)
         }
         case h: HotRodException => h.response
         case c: ClosedChannelException => null
         case _ => createErrorResponseBeforeReadingRequest(t)
      }
   }

   private def createErrorResponseBeforeReadingRequest(t: Throwable): ErrorResponse = {
      logErrorBeforeReadingRequest(t)
      new ErrorResponse(0, 0, "", 1, ServerError, 0, t.toString)
   }

    def createServerException(e: Exception, b: ByteBuf): (HotRodException, Boolean) = {
      e match {
         case i: InvalidMagicIdException =>
            logExceptionReported(i)
            (new HotRodException(new ErrorResponse(
               0, 0, "", 1, InvalidMagicOrMsgId, 0, i.toString), e), true)
         case e: HotRodUnknownOperationException =>
            logExceptionReported(e)
            (new HotRodException(new ErrorResponse(
               e.version, e.messageId, "", 1, UnknownOperation, 0, e.toString), e), true)
         case u: UnknownVersionException =>
            logExceptionReported(u)
            (new HotRodException(new ErrorResponse(
               u.version, u.messageId, "", 1, UnknownVersion, 0, u.toString), e), true)
         case r: RequestParsingException =>
            logExceptionReported(r)
            val msg =
               if (r.getCause == null)
                  r.toString
               else
                  "%s: %s".format(r.getMessage, r.getCause.toString)
            (new HotRodException(new ErrorResponse(
               r.version, r.messageId, "", 1, ParseError, 0, msg), e), true)
         case i: IllegalStateException =>
            // Some internal server code could throw this, so make sure it's logged
            logExceptionReported(i)
            (new HotRodException(decoder.createErrorResponse(header, i), e), false)
         case t: Throwable => (new HotRodException(decoder.createErrorResponse(header, t), e), false)
      }
   }

    def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable)(postCall: => Unit) {
      val ch = ctx.channel
      // Log it just in case the channel is closed or similar
      debug(cause, "Exception caught")
      if (!cause.isInstanceOf[IOException]) {
         val errorResponse = createErrorResponse(cause)
         if (errorResponse != null) {
            errorResponse match {
               case a: Bytes => ch.writeAndFlush(wrappedBuffer(a))
               case cs: CharSequence => ch.writeAndFlush(Unpooled.copiedBuffer(cs, CharsetUtil.UTF_8))
               case null => // ignore
               case _ => ch.writeAndFlush(errorResponse)
            }
         }
      }
      // After writing back an error, reset params and revert to initial state
       postCall
   }

   def replace: Response = {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      var prev = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get(key)
      if (prev != null) {
         // Generate new version only if key present
         prev = cache.replace(key, rawValue, buildMetadata)
      }
      if (prev != null)
         successResp(prev)
      else
         notExecutedResp(prev)
   }

   def obtainCache(cacheManager: EmbeddedCacheManager) = {
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
      this.cache = decoder.getOptimizedCache(header, cache).getAdvancedCache
   }

   private def buildMetadata: Metadata = {
      val metadata = new EmbeddedMetadata.Builder
      metadata.version(generateVersion(server.getCacheRegistry(header.cacheName), cache))
      (params.lifespan, params.maxIdle) match {
         case (EXPIRATION_DEFAULT, EXPIRATION_DEFAULT) =>
            metadata.lifespan(defaultLifespanTime)
            .maxIdle(defaultMaxIdleTime)
         case (_, EXPIRATION_DEFAULT) =>
            metadata.lifespan(toMillis(params.lifespan, params.lifespanNanos))
            .maxIdle(defaultMaxIdleTime)
         case (_, _) =>
            metadata.lifespan(toMillis(params.lifespan, params.lifespanNanos))
            .maxIdle(toMillis(params.maxIdle, params.maxIdleNanos))
      }
      metadata.build()
   }

   def get(keyBytes: Bytes): Response = createGetResponse(cache.getCacheEntry(keyBytes))

   def replaceIfUnmodified: Response = {
      val entry = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntry(key)
      if (entry != null) {
         val prev = entry.getValue
         val streamVersion = new NumericVersion(params.streamVersion)
         if (entry.getMetadata.version() == streamVersion) {
            val v = rawValue
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val replaced = cache.replace(key, prev, v, buildMetadata)
            if (replaced)
               successResp(prev)
            else
               notExecutedResp(prev)
         } else {
            notExecutedResp(prev)
         }
      } else notExistResp
   }

   def putIfAbsent: Response = {
      var prev = cache.get(key)
      if (prev == null) {
         // Generate new version only if key not present
         prev = cache.putIfAbsent(key, rawValue, buildMetadata)
      }
      if (prev == null)
         successResp(prev)
      else
         notExecutedResp(prev)
   }

   def put: Response = {
      // Get an optimised cache in case we can make the operation more efficient
      val prev = cache.put(key, rawValue, buildMetadata)
      successResp(prev)
   }

   def generateVersion(registry: ComponentRegistry, cache: org.infinispan.Cache[Bytes, Bytes]): EntryVersion = {
      val cacheVersionGenerator = registry.getComponent(classOf[VersionGenerator])
      if (cacheVersionGenerator == null) {
         // It could be null, for example when not running in compatibility mode.
         // The reason for that is that if no other component depends on the
         // version generator, the factory does not get invoked.
         val newVersionGenerator = new NumericVersionGenerator()
         .clustered(registry.getComponent(classOf[RpcManager]) != null)
         registry.registerComponent(newVersionGenerator, classOf[VersionGenerator])
         newVersionGenerator.generateNew()
      } else {
         cacheVersionGenerator.generateNew()
      }
   }

   def remove: Response = {
      val prev = cache.remove(key)
      if (prev != null)
         successResp(prev)
      else
         notExistResp
   }

   def successResp(prev: Bytes): Response = decoder.createSuccessResponse(header, prev)

   def notExecutedResp(prev: Bytes): Response = decoder.createNotExecutedResponse(header, prev)

   def notExistResp: Response = decoder.createNotExistResponse(header)

   def createGetResponse(entry: CacheEntry[Bytes, Bytes]): Response = decoder.createGetResponse(header, entry)

   def createMultiGetResponse(pairs: Map[Bytes, CacheEntry[Bytes, Bytes]]): AnyRef = null

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule:
    *
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we subtract the current time in and the
    * result is returned.
    *
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   protected def toMillis(secsDuration: Int, extraNanosDuration: Int): Long = {
      val extraSecond = if (extraNanosDuration > 0) 1 else 0

      val millis = TimeUnit.SECONDS.toMillis(secsDuration) + TimeUnit.NANOSECONDS.toMillis(extraNanosDuration)
      if (secsDuration + extraSecond > SecondsInAMonth) {
         val unixTimeExpiry = millis - System.currentTimeMillis
         if (unixTimeExpiry < 0) 0 else unixTimeExpiry
      } else {
         millis
      }
   }

}

class RequestParameters(val valueLength: Int, val lifespan: Int, val maxIdle: Int, val lifespanNanos: Int, val maxIdleNanos: Int, val streamVersion: Long) {
   override def toString = {
      new StringBuilder().append("RequestParameters").append("{")
      .append("valueLength=").append(valueLength)
      .append(", lifespan=").append(lifespan)
      .append(", lifespanNanos=").append(lifespanNanos)
      .append(", maxIdle=").append(maxIdle)
      .append(", maxIdleNanos=").append(maxIdleNanos)
      .append(", streamVersion=").append(streamVersion)
      .append("}").toString()
   }
}
