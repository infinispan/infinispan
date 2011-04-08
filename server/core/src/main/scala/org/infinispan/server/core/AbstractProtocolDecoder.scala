package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import org.infinispan.stats.Stats
import org.infinispan.server.core.VersionGenerator._
import transport._
import org.infinispan.util.Util
import java.io.StreamCorruptedException
import transport.ExtendedChannelBuffer._
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

/**
 * Common abstract decoder for Memcached and Hot Rod protocols.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolDecoder[K, V <: CacheValue](transport: NettyTransport)
        extends ReplayingDecoder[DecoderState](true) {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters
   type SuitableHeader <: RequestHeader

   private val versionCounter = new AtomicInteger
   private val isTrace = isTraceEnabled

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      var optionalHeader: Option[SuitableHeader] = None
      try {
         optionalHeader = readHeader(buffer)
         if (optionalHeader == None) return null // Something went wrong reading the header, so get more bytes
         val header = optionalHeader.get
         val ret = header.op match {
            case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
               val (k, params) = readKeyAndParams(header, buffer)
               val cache = getCache(header)
               header.op match {
                  case PutRequest => put(header, k, params, cache)
                  case PutIfAbsentRequest => putIfAbsent(header, k, params, cache)
                  case ReplaceRequest => replace(header, k, params, cache)
                  case ReplaceIfUnmodifiedRequest => replaceIfUmodified(header, k, params, cache)
                  case RemoveRequest => remove(header, k, params, cache)
               }
            }
            case GetRequest | GetWithVersionRequest => get(header, buffer, getCache(header))
            case StatsRequest => createStatsResponse(header, getCache(header).getAdvancedCache.getStats)
            case _ => handleCustomRequest(header, buffer, getCache(header), ctx)
         }
         writeResponse(ctx.getChannel, ret)
         null
      } catch {
         case e: Exception => {
            val (serverException, isClientError) = createServerException(e, optionalHeader, buffer)
            // If decode returns an exception, decode won't be called again so,
            // we need to fire the exception explicitly so that requests can
            // carry on being processed on same connection after a client error
            if (isClientError) {
               Channels.fireExceptionCaught(ch, serverException)
               null
            } else {
               throw serverException
            }
         }
         case t: Throwable => throw t
      }
   }

   protected def readKeyAndParams(h: SuitableHeader, b: ChannelBuffer): (K, Option[SuitableParameters]) = {
      val (k, endOfOp) = readKey(h, b)
      val params = if (!endOfOp) readParameters(h, b) else None
      (k, params)
   }

   override def decodeLast(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = null // no-op

   private def writeResponse(ch: Channel, response: AnyRef) {
      if (response != null) {
         if (isTrace) trace("Write response %s", response)
         response match {
            // We only expect Lists of ChannelBuffer instances, so don't worry about type erasure 
            case l: List[ChannelBuffer] => l.foreach(ch.write(_))
            case a: Array[Byte] => ch.write(wrappedBuffer(a))
            case sb: StringBuilder => ch.write(wrappedBuffer(sb.toString.getBytes))
            case s: String => ch.write(wrappedBuffer(s.getBytes))
            case _ => ch.write(response)
         }
      }
   }

   private def put(h: SuitableHeader, k: K, params: Option[SuitableParameters], c: Cache[K, V]): AnyRef = {
      val p = params.get
      val v = createValue(h, p, generateVersion(c))
      // Get an optimised cache in case we can make the operation more efficient
      val prev = getOptimizedCache(h, c).put(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      createSuccessResponse(h, params, prev)
   }

   protected def getOptimizedCache(header: SuitableHeader, c: Cache[K, V]): Cache[K, V] = c

   private def putIfAbsent(header: SuitableHeader, k: K, params: Option[SuitableParameters], c: Cache[K, V]): AnyRef = {
      val p = params.get
      var prev = c.get(k)
      if (prev == null) { // Generate new version only if key not present
         val v = createValue(header, p, generateVersion(c))
         prev = c.putIfAbsent(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      }
      if (prev == null)
         createSuccessResponse(header, params, prev)
      else
         createNotExecutedResponse(header, params, prev)
   }

   private def replace(header: SuitableHeader, k: K, params: Option[SuitableParameters], c: Cache[K, V]): AnyRef = {
      val p = params.get
      var prev = c.get(k)
      if (prev != null) { // Generate new version only if key present
         val v = createValue(header, p, generateVersion(c))
         prev = c.replace(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      }
      if (prev != null)
         createSuccessResponse(header, params, prev)
      else
         createNotExecutedResponse(header, params, prev)
   }

   private def replaceIfUmodified(header: SuitableHeader, k: K, params: Option[SuitableParameters], c: Cache[K, V]): AnyRef = {
      val p = params.get
      val prev = c.get(k)
      if (prev != null) {
         if (prev.version == p.streamVersion) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val v = createValue(header, p, generateVersion(c))
            val replaced = c.replace(k, prev, v);
            if (replaced)
               createSuccessResponse(header, params, prev)
            else
               createNotExecutedResponse(header, params, prev)
         } else {
            createNotExecutedResponse(header, params, prev)
         }            
      } else createNotExistResponse(header, params)
   }

   private def remove(header: SuitableHeader, k: K, params: Option[SuitableParameters], c: Cache[K, V]): AnyRef = {
      val prev = c.remove(k)
      if (prev != null)
         createSuccessResponse(header, params, prev)
      else
         createNotExistResponse(header, params)
   }

   protected def get(header: SuitableHeader, buffer: ChannelBuffer, cache: Cache[K, V]): AnyRef = {
      val (k, endOfOp) = readKey(header, buffer)
      createGetResponse(header, k, cache.get(k))
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Exception reported", e.getCause)
      val ch = ctx.getChannel
      val errorResponse = createErrorResponse(e.getCause)
      if (errorResponse != null) {
         errorResponse match {
            case a: Array[Byte] => ch.write(wrappedBuffer(a))
            case sb: StringBuilder => ch.write(wrappedBuffer(sb.toString.getBytes))
            case null => // ignore
            case _ => ch.write(errorResponse)
         }
      }
   }

   override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      transport.acceptedChannels.add(e.getChannel)
      super.channelOpen(ctx, e)
   }

   protected def readHeader(b: ChannelBuffer): Option[SuitableHeader]

   protected def getCache(h: SuitableHeader): Cache[K, V]

   /**
    * Returns the key read along with a boolean indicating whether the
    * end of the operation was found or not. This allows client to
    * differentiate between extra parameters or pipelined sequence of
    * operations.
    */
   protected def readKey(h: SuitableHeader, b: ChannelBuffer): (K, Boolean)

   protected def readParameters(h: SuitableHeader, b: ChannelBuffer): Option[SuitableParameters]

   protected def createValue(h: SuitableHeader, p: SuitableParameters, nextVersion: Long): V

   protected def createSuccessResponse(h: SuitableHeader, params: Option[SuitableParameters], prev: V): AnyRef

   protected def createNotExecutedResponse(h: SuitableHeader, params: Option[SuitableParameters], prev: V): AnyRef

   protected def createNotExistResponse(h: SuitableHeader, params: Option[SuitableParameters]): AnyRef

   protected def createGetResponse(h: SuitableHeader, k: K, v: V): AnyRef

   protected def createMultiGetResponse(h: SuitableHeader, pairs: Map[K, V]): AnyRef
   
   protected def createErrorResponse(t: Throwable): AnyRef

   protected def createStatsResponse(h: SuitableHeader, stats: Stats): AnyRef

   protected def handleCustomRequest(h: SuitableHeader, b: ChannelBuffer, cache: Cache[K, V],
                                     ctx: ChannelHandlerContext): AnyRef

   protected def createServerException(e: Exception, h: Option[SuitableHeader], b: ChannelBuffer): (Exception, Boolean)

   protected def generateVersion(cache: Cache[K, V]): Long = {
      val rpcManager = cache.getAdvancedCache.getRpcManager
      newVersion(rpcManager != null)
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule:
    *
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we substract the current time in and the
    * result is returned.
    *
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   protected def toMillis(lifespan: Int): Long = {
      if (lifespan > SecondsInAMonth) {
         val unixTimeExpiry = TimeUnit.SECONDS.toMillis(lifespan) - System.currentTimeMillis
         if (unixTimeExpiry < 0) 0 else unixTimeExpiry
      } else {
         TimeUnit.SECONDS.toMillis(lifespan)
      }
   }

}

object AbstractProtocolDecoder extends Logging {
   private val SecondsInAMonth = 60 * 60 * 24 * 30
   private val DefaultTimeUnit = TimeUnit.MILLISECONDS
}

class RequestHeader(val op: Enumeration#Value) {
   override def toString = {
      new StringBuilder().append("RequestHeader").append("{")
         .append("op=").append(op)
         .append("}").toString
   }
}

class RequestParameters(val data: Array[Byte], val lifespan: Int, val maxIdle: Int, val streamVersion: Long) {
   override def toString = {
      new StringBuilder().append("RequestParameters").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append(", lifespan=").append(lifespan)
         .append(", maxIdle=").append(maxIdle)
         .append(", streamVersion=").append(streamVersion)
         .append("}").toString
   }
}

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)
