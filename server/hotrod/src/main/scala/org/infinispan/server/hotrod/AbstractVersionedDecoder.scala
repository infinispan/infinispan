package org.infinispan.server.hotrod

import java.util.concurrent.TimeUnit

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import org.infinispan.configuration.cache.Configuration
import org.infinispan.container.entries.CacheEntry
import org.infinispan.server.core.transport.NettyTransport

/**
 * This class represents the work to be done by a decoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractVersionedDecoder {

   val SecondsIn30days = 60 * 60 * 24 * 30

   /**
    * Having read the message's Id, read the rest of Hot Rod header from the given buffer and return it. Returns
    * whether the entire header was read or not.
    */
   @throws(classOf[Exception])
   def readHeader(buffer: ByteBuf, version: Byte, messageId: Long, header: HotRodHeader): Boolean

   /**
    * Read the parameters of the operation, if present.
    */
   def readParameters(header: HotRodHeader, buffer: ByteBuf): Option[RequestParameters]

   /**
    * Create a successful response.
    */
   def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): Response

   /**
    * Create a response indicating the the operation could not be executed.
    */
   def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): Response

   /**
    * Create a response indicating that the key, which the message tried to operate on, did not exist.
    */
   def createNotExistResponse(header: HotRodHeader): Response

   /**
    * Create a response for get a request.
    */
   def createGetResponse(header: HotRodHeader, entry: CacheEntry[Array[Byte], Array[Byte]]): Response

  /**
    * Read operation specific data for an operation that only requires a header
    */
   def customReadHeader(header: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext, out: java.util.List[AnyRef]): Unit

   /**
    * Handle a protocol specific key reading.
    */
   def customReadKey(header: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext, out: java.util.List[AnyRef]): Unit

   /**
    * Handle a protocol specific value reading.
    */
   def customReadValue(header: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext, out: java.util.List[AnyRef]): Unit

   /**
    * Create a response for the stats command.
    */
   def createStatsResponse(hrCtx: CacheDecodeContext, t: NettyTransport): StatsResponse

   /**
    * Create an error response based on the Throwable instance received.
    */
   def createErrorResponse(header: HotRodHeader, t: Throwable): ErrorResponse

   /**
    * Get an optimized cache instance depending on the operation parameters.
    */
   def getOptimizedCache(h: HotRodHeader, c: Cache, cacheCfg: Configuration): Cache

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule (inspired by Memcached):
    *
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we subtract the current time in and the
    * result is returned.
    *
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   def toMillis(param: ExpirationParam, h: HotRodHeader): Long = {
      if (param.duration > 0) {
         if (param.duration > SecondsIn30days) {
            val unixTimeExpiry = TimeUnit.SECONDS.toMillis(param.duration) - System.currentTimeMillis
            if (unixTimeExpiry < 0) 0 else unixTimeExpiry
         } else {
            TimeUnit.SECONDS.toMillis(param.duration)
         }
      } else {
         param.duration
      }
   }

}
