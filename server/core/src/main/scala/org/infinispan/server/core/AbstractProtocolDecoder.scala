package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import scala.collection.mutable.HashMap
import scala.collection.immutable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit
import org.infinispan.stats.Stats
import org.infinispan.server.core.VersionGenerator._
import java.io.StreamCorruptedException
import transport._
import transport.ChannelBuffers._
import org.infinispan.util.Util

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolDecoder[K, V <: CacheValue] extends Decoder {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters
   type SuitableHeader <: RequestHeader

   private val versionCounter = new AtomicInteger

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): AnyRef = {
      if (buffer.readableBytes < 1) {
         trace("No bytes to decode")
         return null
      }
      val header = readHeader(buffer)
      if (header == null) return null // Something went wrong reading the header, so get more bytes 
      try {
         val cache = getCache(header)
         val ret = header.op match {
            case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
               val k = readKey(header, buffer)
               val params = readParameters(header, buffer)
               header.op match {
                  case PutRequest => put(header, k, params, cache)
                  case PutIfAbsentRequest => putIfAbsent(header, k, params, cache)
                  case ReplaceRequest => replace(header, k, params, cache)
                  case ReplaceIfUnmodifiedRequest => replaceIfUmodified(header, k, params, cache)
                  case RemoveRequest => remove(header, k, params, cache)
               }
            }
            case GetRequest | GetWithVersionRequest => get(header, buffer, cache)
            case StatsRequest => createStatsResponse(header, cache.getAdvancedCache.getStats)
            case _ => handleCustomRequest(header, buffer, cache)
         }
         writeResponse(ctx.getChannel, ret)
         null
      } catch {
         case se: ServerException => throw se
         case e: Exception => throw new ServerException(header, e)
         case t: Throwable => throw t
      }
   }

   private def writeResponse(ch: Channel, response: AnyRef) {
      if (response != null) {
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

   private def put(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val p = params.get
      val v = createValue(header, p, generateVersion(cache))
      val prev = cache.put(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      createSuccessResponse(header, params, prev)
   }

   private def putIfAbsent(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val p = params.get
      val prev = cache.get(k)
      if (prev == null) { // Generate new version only if key not present      
         val v = createValue(header, p, generateVersion(cache))
         cache.putIfAbsent(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      }
      if (prev == null)
         createSuccessResponse(header, params, prev)
      else
         createNotExecutedResponse(header, params, prev)
   }

   private def replace(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val p = params.get
      val prev = cache.get(k)
      if (prev != null) { // Generate new version only if key present
         val v = createValue(header, p, generateVersion(cache))
         cache.replace(k, v, toMillis(p.lifespan), DefaultTimeUnit, toMillis(p.maxIdle), DefaultTimeUnit)
      }
      if (prev != null)
         createSuccessResponse(header, params, prev)
      else
         createNotExecutedResponse(header, params, prev)
   }

   private def replaceIfUmodified(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val p = params.get
      val prev = cache.get(k)
      if (prev != null) {
         if (prev.version == p.streamVersion) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val v = createValue(header, p, generateVersion(cache))
            val replaced = cache.replace(k, prev, v);
            if (replaced)
               createSuccessResponse(header, params, prev)
            else
               createNotExecutedResponse(header, params, prev)
         } else {
            createNotExecutedResponse(header, params, prev)
         }            
      } else createNotExistResponse(header, params)
   }

   private def remove(header: SuitableHeader, k: K, params: Option[SuitableParameters], cache: Cache[K, V]): AnyRef = {
      val prev = cache.remove(k)
      if (prev != null)
         createSuccessResponse(header, params, prev)
      else
         createNotExistResponse(header, params)
   }

   private def get(header: SuitableHeader, buffer: ChannelBuffer, cache: Cache[K, V]): AnyRef = {
      val keys = readKeys(header, buffer)
      if (keys.length > 1) {
         val map = new HashMap[K,V]()
         for (k <- keys) {
            val v = cache.get(k)
            if (v != null)
               map += (k -> v)
         }
         createMultiGetResponse(header, new immutable.HashMap ++ map)
      } else {
         createGetResponse(header, keys.head, cache.get(keys.head))
      }
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

   protected def readHeader(b: ChannelBuffer): SuitableHeader

   protected def getCache(h: SuitableHeader): Cache[K, V]

   protected def readKey(h: SuitableHeader, b: ChannelBuffer): K

   protected def readKeys(h: SuitableHeader, b: ChannelBuffer): Array[K]

   protected def readParameters(h: SuitableHeader, b: ChannelBuffer): Option[SuitableParameters]

   protected def createValue(h: SuitableHeader, p: SuitableParameters, nextVersion: Long): V

   protected def createSuccessResponse(h: SuitableHeader, params: Option[SuitableParameters], prev: V): AnyRef

   protected def createNotExecutedResponse(h: SuitableHeader, params: Option[SuitableParameters], prev: V): AnyRef

   protected def createNotExistResponse(h: SuitableHeader, params: Option[SuitableParameters]): AnyRef

   protected def createGetResponse(h: SuitableHeader, k: K, v: V): AnyRef

   protected def createMultiGetResponse(h: SuitableHeader, pairs: Map[K, V]): AnyRef
   
   protected def createErrorResponse(t: Throwable): AnyRef

   protected def createStatsResponse(h: SuitableHeader, stats: Stats): AnyRef

   protected def handleCustomRequest(h: SuitableHeader, b: ChannelBuffer, cache: Cache[K, V]): AnyRef

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

class ServerException(val header: RequestHeader, cause: Throwable) extends Exception(cause)