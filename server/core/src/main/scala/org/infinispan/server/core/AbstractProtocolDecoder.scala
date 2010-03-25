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
                  case PutRequest => {
                     putInCache(header, k, params.get, cache)
                     sendResponse(header, ctx, None, None, params, None)
                  }
                  case PutIfAbsentRequest => {
                     val prev = cache.get(k)
                     if (prev == null) putIfAbsentInCache(header, k, params.get, cache) // Generate new version only if key not present
                     sendResponse(header, ctx, None, None, params, Some(prev))
                  }
                  case ReplaceRequest => {
                     val prev = cache.get(k)
                     if (prev != null) replaceInCache(header, k, params.get, cache) // Generate new version only if key present
                     sendResponse(header, ctx, None, None, params, Some(prev))
                  }
                  case ReplaceIfUnmodifiedRequest => {
                     val prev = cache.get(k)
                     if (prev != null) {
                        if (prev.version == params.get.streamVersion) {
                           // Generate new version only if key present and version has not changed, otherwise it's wasteful
                           val v = createValue(header, params.get, generateVersion(cache))
                           val replaced = cache.replace(k, prev, v);
                           if (replaced)
                              sendResponse(header, ctx, None, Some(v), params, Some(prev))
                           else
                              sendResponse(header, ctx, None, None, params, Some(prev))
                        } else {
                           sendResponse(header, ctx, None, None, params, Some(prev))
                        }
                     } else {
                        sendResponse(header, ctx, None, None, params, None)
                     }
                  }
                  case RemoveRequest => {
                     val prev = cache.remove(k)
                     sendResponse(header, ctx, None, None, params, Some(prev))
                  }
               }
            }
            case GetRequest | GetWithVersionRequest => {
               val keys = readKeys(header, buffer)
               if (keys.length > 1) {
                  val map = new HashMap[K,V]()
                  for (k <- keys) {
                     val v = cache.get(k)
                     if (v != null)
                        map += (k -> v)
                  }
                  sendMultiGetResponse(header, ctx, new immutable.HashMap ++ map)
               } else {
                  sendResponse(header, ctx, Some(keys.head), Some(cache.get(keys.head)), None, None)
               }
            }
            case StatsRequest => sendResponse(header, ctx, cache.getAdvancedCache.getStats)
            case _ => handleCustomRequest(header, ctx, buffer, cache)
         }
         // TODO: to avoid checking for null, make all send* methods return something, even the memcached ones,
         // they could send back a buffer whereas hotrod would send back pojos.
         if (ret != null) ctx.getChannel.write(ret)
         null
      } catch {
         case se: ServerException => throw se
         case e: Exception => throw new ServerException(header, e)
         case t: Throwable => throw t
      }
   }

   private def putInCache(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(header, params, generateVersion(cache))
      cache.put(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   private def putIfAbsentInCache(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(header, params, generateVersion(cache))
      cache.putIfAbsent(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   private def replaceInCache(header: SuitableHeader, k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(header, params, generateVersion(cache))
      cache.replace(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Exception reported", e.getCause)
      sendResponse(ctx, e.getCause)
   }

   def readHeader(buffer: ChannelBuffer): SuitableHeader

   def getCache(header: SuitableHeader): Cache[K, V]

   // todo: probably remove in favour of readKeys
   def readKey(header: SuitableHeader, buffer: ChannelBuffer): K

   def readKeys(header: SuitableHeader, buffer: ChannelBuffer): Array[K]

   def readParameters(header: SuitableHeader, buffer: ChannelBuffer): Option[SuitableParameters]

   def createValue(header: SuitableHeader, params: SuitableParameters, nextVersion: Long): V 

   def sendResponse(header: SuitableHeader, ctx: ChannelHandlerContext, k: Option[K], v: Option[V],
                    params: Option[SuitableParameters], prev: Option[V]): AnyRef = {
      val buffers = ctx.getChannelBuffers
      val ch = ctx.getChannel
      if (params == None || !params.get.noReply) {
         // TODO consolidate this event further since both hotrod and memcached end up writing to a channel something,
         // this methods here could just simply create the responses and let the common framework write them
         val ret = header.op match {
            case PutRequest => sendPutResponse(header, ch, buffers)
            case GetRequest | GetWithVersionRequest => sendGetResponse(header, ch, buffers, k.get, v.get)
            case PutIfAbsentRequest => sendPutIfAbsentResponse(header, ch, buffers, prev.get)
            case ReplaceRequest => sendReplaceResponse(header, ch, buffers, prev.get)
            case ReplaceIfUnmodifiedRequest => sendReplaceIfUnmodifiedResponse(header, ch, buffers, v, prev)
            case RemoveRequest => sendRemoveResponse(header, ch, buffers, prev.get)
//            case _ => sendCustomResponse(header, ch, buffers, v, prev)
         }
         ret
      } else null
   }

   def sendPutResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers): AnyRef

   def sendGetResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers, k: K, v: V): AnyRef

   def sendPutIfAbsentResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers, prev: V): AnyRef

   def sendReplaceResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers, prev: V): AnyRef

   def sendReplaceIfUnmodifiedResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers,
                                       v: Option[V], prev: Option[V]): AnyRef

   def sendRemoveResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers, prev: V): AnyRef

   def sendMultiGetResponse(header: SuitableHeader, ctx: ChannelHandlerContext, pairs: Map[K, V]): AnyRef
   
//   def sendCustomResponse(header: SuitableHeader, ch: Channel, buffers: ChannelBuffers, v: Option[V], prev: Option[V]): AnyRef

   def sendResponse(ctx: ChannelHandlerContext, t: Throwable): AnyRef

   def sendResponse(header: SuitableHeader, ctx: ChannelHandlerContext, stats: Stats): AnyRef

   def handleCustomRequest(header: SuitableHeader, ctx: ChannelHandlerContext,
                           buffer: ChannelBuffer, cache: Cache[K, V]): AnyRef

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

   protected def generateVersion(cache: Cache[K, V]): Long = {
      val rpcManager = cache.getAdvancedCache.getRpcManager
      if (rpcManager != null) {
         val transport = rpcManager.getTransport
         newVersion(Some(transport.getAddress), Some(transport.getMembers), transport.getViewId)
      } else {
         newVersion(None, None, 0)
      }
   }

}

object AbstractProtocolDecoder extends Logging {
   private val SecondsInAMonth = 60 * 60 * 24 * 30
   private val DefaultTimeUnit = TimeUnit.MILLISECONDS 
}

class RequestHeader(val op: Enumeration#Value)

class RequestParameters(val data: Array[Byte], val lifespan: Int, val maxIdle: Int, val streamVersion: Long, val noReply: Boolean)

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)

class ServerException(val header: RequestHeader, cause: Throwable) extends Exception(cause)