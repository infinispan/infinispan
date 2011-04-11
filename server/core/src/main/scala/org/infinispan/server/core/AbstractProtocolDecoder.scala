package org.infinispan.server.core

import org.infinispan.Cache
import Operation._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import org.infinispan.server.core.VersionGenerator._
import transport._
import org.infinispan.util.Util
import java.io.StreamCorruptedException
import transport.ExtendedChannelBuffer._
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import DecoderState._

/**
 * Common abstract decoder for Memcached and Hot Rod protocols.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolDecoder[K, V <: CacheValue](transport: NettyTransport)
        extends ReplayingDecoder[DecoderState](READ_HEADER, true) {
   import AbstractProtocolDecoder._

   type SuitableParameters <: RequestParameters
   type SuitableHeader <: RequestHeader

   private val versionCounter = new AtomicInteger
   private val isTrace = isTraceEnabled

   protected var header: SuitableHeader = null.asInstanceOf[SuitableHeader]
   protected var params: SuitableParameters = null.asInstanceOf[SuitableParameters]
   protected var k: K = null.asInstanceOf[K]
   protected var cache: Cache[K, V] = null

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val ch = ctx.getChannel
      try {
         state match {
            case READ_HEADER => readHeader(ch, buffer, state)
            case READ_KEY => readKey(ch, buffer, state)
            case READ_VALUE => readValue(ch, buffer, state)
         }
      } catch {
         case e: Exception => {
            val (serverException, isClientError) = createServerException(e, buffer)
            // If decode returns an exception, decode won't be called again so,
            // we need to fire the exception explicitly so that requests can
            // carry on being processed on same connection after a client error
            if (isClientError) {
               Channels.fireExceptionCaught(ch, serverException)
               checkpointTo(READ_HEADER)
               null
            } else {
               throw serverException
            }
         }
         case t: Throwable => throw t
      }
   }

   private def readHeader(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val (optHeader, endOfOp) = readHeader(buffer)
      if (optHeader == None) return null // Something went wrong reading the header, so get more bytes
      header = optHeader.get
      cache = getCache
      if (endOfOp) {
         header.op match {
            case StatsRequest => writeResponse(ch, createStatsResponse)
            case _ => customReadHeader(ch, buffer)
         }
      } else {
         checkpointTo(READ_KEY)
      }
   }

   private def readKey(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
            val (key, endOfOp) = readKey(buffer)
            k = key
            if (endOfOp) {
               // If it's the end of the operation, it can only be a remove
               writeResponse(ch, remove)
            } else {
               checkpointTo(READ_VALUE)
            }
         }
         case GetRequest | GetWithVersionRequest => writeResponse(ch, get(buffer))
         case _ => customReadKey(ch, buffer)
      }
   }

   private def readValue(ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = {
      val ret = header.op match {
         case PutRequest | PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest | RemoveRequest => {
            readParameters(buffer)
            header.op match {
               case PutRequest => put
               case PutIfAbsentRequest => putIfAbsent
               case ReplaceRequest => replace
               case ReplaceIfUnmodifiedRequest => replaceIfUmodified
               case RemoveRequest => remove
            }
         }
         case _ => customReadValue(ch, buffer)
      }
      writeResponse(ch, ret)
   }

   override def decodeLast(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: DecoderState): AnyRef = null // no-op

   protected def writeResponse(ch: Channel, response: AnyRef): AnyRef = {
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
      checkpointTo(READ_HEADER)
   }

   private def put: AnyRef = {
      val v = createValue(generateVersion(cache))
      // Get an optimised cache in case we can make the operation more efficient
      val prev = getOptimizedCache(cache).put(k, v,
         toMillis(params.lifespan), DefaultTimeUnit,
         toMillis(params.maxIdle), DefaultTimeUnit)
      createSuccessResponse(prev)
   }

   protected def getOptimizedCache(c: Cache[K, V]): Cache[K, V] = c

   private def putIfAbsent: AnyRef = {
      var prev = cache.get(k)
      if (prev == null) { // Generate new version only if key not present
         val v = createValue(generateVersion(cache))
         prev = cache.putIfAbsent(k, v,
            toMillis(params.lifespan), DefaultTimeUnit,
            toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (prev == null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   private def replace: AnyRef = {
      var prev = cache.get(k)
      if (prev != null) { // Generate new version only if key present
         val v = createValue(generateVersion(cache))
         prev = cache.replace(k, v,
            toMillis(params.lifespan), DefaultTimeUnit,
            toMillis(params.maxIdle), DefaultTimeUnit)
      }
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExecutedResponse(prev)
   }

   private def replaceIfUmodified: AnyRef = {
      val prev = cache.get(k)
      if (prev != null) {
         if (prev.version == params.streamVersion) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val v = createValue(generateVersion(cache))
            val replaced = cache.replace(k, prev, v);
            if (replaced)
               createSuccessResponse(prev)
            else
               createNotExecutedResponse(prev)
         } else {
            createNotExecutedResponse(prev)
         }            
      } else createNotExistResponse
   }

   private def remove: AnyRef = {
      val prev = cache.remove(k)
      if (prev != null)
         createSuccessResponse(prev)
      else
         createNotExistResponse
   }

   protected def get(buffer: ChannelBuffer): AnyRef =
      createGetResponse(k, cache.get(readKey(buffer)._1))

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

   def checkpointTo(state: DecoderState): AnyRef = {
      checkpoint(state)
      null // For netty's decoder that mandates a return
   }

   protected def readHeader(b: ChannelBuffer): (Option[SuitableHeader], Boolean)

   protected def getCache: Cache[K, V]

   /**
    * Returns the key read along with a boolean indicating whether the
    * end of the operation was found or not. This allows client to
    * differentiate between extra parameters or pipelined sequence of
    * operations.
    */
   protected def readKey(b: ChannelBuffer): (K, Boolean)

   protected def readParameters(b: ChannelBuffer)

   protected def createValue(nextVersion: Long): V

   protected def createSuccessResponse(prev: V): AnyRef

   protected def createNotExecutedResponse(prev: V): AnyRef

   protected def createNotExistResponse: AnyRef

   protected def createGetResponse(k: K, v: V): AnyRef

   protected def createMultiGetResponse(pairs: Map[K, V]): AnyRef
   
   protected def createErrorResponse(t: Throwable): AnyRef

   protected def createStatsResponse: AnyRef

   protected def customReadHeader(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def customReadKey(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def customReadValue(ch: Channel, buffer: ChannelBuffer): AnyRef

   protected def createServerException(e: Exception, b: ChannelBuffer): (Exception, Boolean)

   protected def generateVersion(cache: Cache[K, V]): Long =
      newVersion(cache.getAdvancedCache.getRpcManager != null)

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
