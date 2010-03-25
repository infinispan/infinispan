package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import scala.collection.mutable.HashMap
import scala.collection.immutable
import org.infinispan.remoting.transport.Address
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit
import org.infinispan.stats.Stats
import org.infinispan.server.core.VersionGenerator._
import java.io.StreamCorruptedException
import transport._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class AbstractProtocolDecoder[K, V <: CacheValue] extends Decoder {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters
   type SuitableHeader <: RequestHeader

   private val versionCounter = new AtomicInteger

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): AnyRef = {
      if (buffer.readableBytes < 1) return null
      val header = readHeader(buffer)
      if (header == null) return null // Something went wrong reading the header, so get more bytes 
      try {
         val cache = getCache(header)
         val ret = header.op match {
            case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
               val k = readKey(header, buffer)
               val params = readParameters(header, buffer)
               header.op match {
                  case PutRequest => put(header, k, params.get, cache)
                  case PutIfAbsentRequest => putIfAbsent(header, k, params.get, cache)
                  case ReplaceRequest => replace(header, k, params.get, cache)
                  case ReplaceIfUnmodifiedRequest => replaceIfUmodified(header, k, params.get, cache)
                  case RemoveRequest => remove(header, k, params, cache)
               }
            }
            case GetRequest | GetWithVersionRequest => get(header, buffer, ctx.getChannelBuffers, cache)
            case StatsRequest => createStatsResponse(header, ctx.getChannelBuffers, cache.getAdvancedCache.getStats)
            case _ => handleCustomRequest(header, ctx, buffer, cache)
         }
         writeResponse(ctx.getChannel, ctx.getChannelBuffers, ret)
         null
      } catch {
         case se: ServerException => throw se
         case e: Exception => throw new ServerException(header, e)
         case t: Throwable => throw t
      }
   }

   private def writeResponse(ch: Channel, buffers: ChannelBuffers, response: AnyRef) {
      if (response != null) {
         response match {
            case l: List[ChannelBuffer] => l.foreach(ch.write(_))
            case a: Array[Byte] => ch.write(buffers.wrappedBuffer(a))
            case sb: StringBuilder => ch.write(buffers.wrappedBuffer(sb.toString.getBytes))
            case s: String => ch.write(buffers.wrappedBuffer(s.getBytes))
            case _ => ch.write(response)
         }
      }
   }

   private def put(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): AnyRef = {
      val v = createValue(header, params, generateVersion(cache))
      cache.put(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
      if (!params.noReply) createSuccessResponse(header) else null
   }

   private def putIfAbsent(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): AnyRef = {
      val prev = cache.get(k)
      if (prev == null) { // Generate new version only if key not present      
         val v = createValue(header, params, generateVersion(cache))
         cache.putIfAbsent(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (!params.noReply && prev == null)
         createSuccessResponse(header)
      else if (!params.noReply && prev != null)
         createNotExecutedResponse(header)
      else
         null
   }

   private def replace(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): AnyRef = {
      val prev = cache.get(k)
      if (prev != null) { // Generate new version only if key present
         val v = createValue(header, params, generateVersion(cache))
         cache.replace(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (!params.noReply && prev != null)
         createSuccessResponse(header)
      else if (!params.noReply && prev == null)
         createNotExecutedResponse(header)
      else
         null
   }

   private def replaceIfUmodified(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): AnyRef = {
      val prev = cache.get(k)
      if (prev != null) {
         if (prev.version == params.streamVersion) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val v = createValue(header, params, generateVersion(cache))
            val replaced = cache.replace(k, prev, v);
            if (!params.noReply && replaced)
               createSuccessResponse(header)
            else if (!params.noReply)
               createNotExecutedResponse(header)
            else
               null
         } else if (!params.noReply) {
            createNotExecutedResponse(header)
         } else {
            null
         }
      } else if(!params.noReply) {
         createNotExistResponse(header)
      } else {
         null
      }
   }

   private def remove(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val prev = cache.remove(k)
      if ((params == None || !params.get.noReply) && prev != null)
         createSuccessResponse(header)
      else if ((params == None || !params.get.noReply) && prev == null)
         createNotExistResponse(header)
      else
         null
   }

   private def get(header: SuitableHeader, buffer: ChannelBuffer, buffers: ChannelBuffers, cache: Cache[K, V]): AnyRef = {
      val keys = readKeys(header, buffer)
      if (keys.length > 1) {
         val map = new HashMap[K,V]()
         for (k <- keys) {
            val v = cache.get(k)
            if (v != null)
               map += (k -> v)
         }
         createMultiGetResponse(header, buffers, new immutable.HashMap ++ map)
      } else {
         createGetResponse(header, buffers, keys.head, cache.get(keys.head))
      }
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Exception reported", e.getCause)
      val ch = ctx.getChannel
      val buffers = ctx.getChannelBuffers
      val errorResponse = createErrorResponse(e.getCause)
      if (errorResponse != null) {
         errorResponse match {
            case a: Array[Byte] => ch.write(buffers.wrappedBuffer(a))
            case sb: StringBuilder => ch.write(buffers.wrappedBuffer(sb.toString.getBytes))
            case _ => ch.write(errorResponse)
         }
      }
   }

   protected def readHeader(buffer: ChannelBuffer): SuitableHeader

   protected def getCache(header: SuitableHeader): Cache[K, V]

   protected def readKey(header: SuitableHeader, buffer: ChannelBuffer): K

   protected def readKeys(header: SuitableHeader, buffer: ChannelBuffer): Array[K]

   protected def readParameters(header: SuitableHeader, buffer: ChannelBuffer): Option[SuitableParameters]

   protected def createValue(header: SuitableHeader, params: SuitableParameters, nextVersion: Long): V

   protected def createSuccessResponse(header: SuitableHeader): AnyRef

   protected def createNotExecutedResponse(header: SuitableHeader): AnyRef

   protected def createNotExistResponse(header: SuitableHeader): AnyRef

   protected def createGetResponse(header: SuitableHeader, buffers: ChannelBuffers, k: K, v: V): AnyRef

   protected def createMultiGetResponse(header: SuitableHeader, buffers: ChannelBuffers, pairs: Map[K, V]): AnyRef
   
   protected def createErrorResponse(t: Throwable): AnyRef

   protected def createStatsResponse(header: SuitableHeader, buffers: ChannelBuffers, stats: Stats): AnyRef

   protected def handleCustomRequest(header: SuitableHeader, ctx: ChannelHandlerContext,
                           buffer: ChannelBuffer, cache: Cache[K, V]): AnyRef

   protected def generateVersion(cache: Cache[K, V]): Long = {
      val rpcManager = cache.getAdvancedCache.getRpcManager
      if (rpcManager != null) {
         val transport = rpcManager.getTransport
         newVersion(Some(transport.getAddress), Some(transport.getMembers), transport.getViewId)
      } else {
         newVersion(None, None, 0)
      }
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
   private def toMillis(lifespan: Int): Long = {
      if (lifespan > SecondsInAMonth) TimeUnit.SECONDS.toMillis(lifespan) - System.currentTimeMillis
      else TimeUnit.SECONDS.toMillis(lifespan)
   }

}

object AbstractProtocolDecoder extends Logging {
   private val SecondsInAMonth = 60 * 60 * 24 * 30
   private val DefaultTimeUnit = TimeUnit.MILLISECONDS 
}

class RequestHeader(val op: Enumeration#Value)

// TODO: NoReply could possibly be passed to subclass specific to memcached and make create* implementations use it
class RequestParameters(val data: Array[Byte], val lifespan: Int, val maxIdle: Int, val streamVersion: Long, val noReply: Boolean)

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)

class ServerException(val header: RequestHeader, cause: Throwable) extends Exception(cause)