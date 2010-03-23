package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import transport.{ExceptionEvent, Decoder, ChannelHandlerContext, ChannelBuffer}
import scala.collection.mutable.HashMap
import scala.collection.immutable
import org.infinispan.remoting.transport.Address
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit
import org.infinispan.stats.Stats
import org.infinispan.server.core.VersionGenerator._
import java.io.StreamCorruptedException

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class AbstractProtocolDecoder[K, V <: Value] extends Decoder {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters

   private val versionCounter = new AtomicInteger

   override def decode(ctx: ChannelHandlerContext, buffer: ChannelBuffer): AnyRef = {
      if (buffer.readableBytes < 1) return null
      val header = readHeader(buffer)
      val cache = getCache(header)
      header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | DeleteRequest => {
            val k = readKey(buffer)
            val params = readParameters(header.op, buffer)
            header.op match {
               case PutRequest => {
                  putInCache(k, params, cache)
                  sendResponse(header, ctx, None, None, Some(params), None)
               }
               case PutIfAbsentRequest => {
                  val prev = cache.get(k)
                  if (prev == null) putIfAbsentInCache(k, params, cache) // Generate new version only if key not present
                  sendResponse(header, ctx, None, None, Some(params), Some(prev))
               }
               case ReplaceRequest => {
                  val prev = cache.get(k)
                  if (prev != null) replaceInCache(k, params, cache) // Generate new version only if key present
                  sendResponse(header, ctx, None, None, Some(params), Some(prev))
               }
               case ReplaceIfUnmodifiedRequest => {
                  val prev = cache.get(k)
                  if (prev != null) {
                     if (prev.version == params.version) {
                        // Generate new version only if key present and version has not changed, otherwise it's wasteful
                        val v = createValue(params, generateVersion(cache))
                        val replaced = cache.replace(k, prev, v);
                        if (replaced)
                           sendResponse(header, ctx, None, Some(v), Some(params), Some(prev))
                        else
                           sendResponse(header, ctx, None, None, Some(params), Some(prev))
                     } else {
                        sendResponse(header, ctx, None, None, Some(params), Some(prev))
                     }
                  } else {
                     sendResponse(header, ctx, None, None, Some(params), None)
                  }
               }
               case DeleteRequest => {
                  val prev = cache.remove(k)
                  sendResponse(header, ctx, None, None, Some(params), Some(prev))
               }
            }
         }
         case GetRequest | GetWithVersionRequest => {
            val keys = readKeys(buffer)
            if (keys.length > 1) {
               val map = new HashMap[K,V]()
               for (k <- keys) {
                  val v = cache.get(k)
                  if (v != null)
                     map += (k -> v)
               }
               sendResponse(header, ctx, new immutable.HashMap ++ map)
            } else {
               sendResponse(header, ctx, Some(keys.head), Some(cache.get(keys.head)), None, None)
            }
         }
         case StatsRequest => sendResponse(ctx, cache.getAdvancedCache.getStats)
         case _ => handleCustomRequest(header, ctx, buffer, cache)
      }
      null
   }

   private def putInCache(k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(params, generateVersion(cache))
      cache.put(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   private def putIfAbsentInCache(k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(params, generateVersion(cache))
      cache.putIfAbsent(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   private def replaceInCache(k: K, params: SuitableParameters, cache: Cache[K, V]): V = {
      val v = createValue(params, generateVersion(cache))
      cache.replace(k, v, toMillis(params.lifespan), DefaultTimeUnit, toMillis(params.maxIdle), DefaultTimeUnit)
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Exception reported", e.getCause)
      sendResponse(ctx, e.getCause)
   }

   def readHeader(buffer: ChannelBuffer): RequestHeader

   def getCache(header: RequestHeader): Cache[K, V]

   // todo: probably remove in favour of readKeys
   def readKey(buffer: ChannelBuffer): K

   def readKeys(buffer: ChannelBuffer): Array[K]

   def readParameters(op: Enumeration#Value, buffer: ChannelBuffer): SuitableParameters

   def createValue(params: SuitableParameters, nextVersion: Long): V 

   def sendResponse(header: RequestHeader, ctx: ChannelHandlerContext, k: Option[K], v: Option[V], params: Option[SuitableParameters], prev: Option[V])

   def sendResponse(header: RequestHeader, ctx: ChannelHandlerContext, pairs: Map[K, V])

   def sendResponse(ctx: ChannelHandlerContext, t: Throwable)

   def sendResponse(ctx: ChannelHandlerContext, stats: Stats)

   def handleCustomRequest(header: RequestHeader, ctx: ChannelHandlerContext, buffer: ChannelBuffer, cache: Cache[K, V])

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

// todo: once I implement the hotrod see, revisit to see whether this class is still necessary,
// todo: ...I suspect so cos I'd need a place to hold stuff that appears before the operation
class RequestHeader(val op: Enumeration#Value)

class RequestParameters(val data: Array[Byte], val lifespan: Int, val maxIdle: Int, val version: Long)

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)