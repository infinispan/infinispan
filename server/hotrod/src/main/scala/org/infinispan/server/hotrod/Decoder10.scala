package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._
import HotRodOperation._
import OperationStatus._
import org.infinispan.server.core.transport.{ChannelBuffer}
import org.infinispan.Cache
import org.infinispan.stats.Stats
import org.infinispan.server.core._
import collection.mutable
import collection.immutable
import org.infinispan.util.concurrent.TimeoutException
import java.io.IOException
import org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP
import org.infinispan.util.ByteArrayKey

/**
 * HotRod protocol decoder specific for specification version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object Decoder10 extends AbstractVersionedDecoder with Logging {
   import OperationResponse._
   import ProtocolFlag._
   type SuitableHeader = HotRodHeader

   override def readHeader(buffer: ChannelBuffer, messageId: Long): HotRodHeader = {
      val streamOp = buffer.readUnsignedByte
      val op = streamOp match {
         case 0x01 => PutRequest
         case 0x03 => GetRequest
         case 0x05 => PutIfAbsentRequest
         case 0x07 => ReplaceRequest
         case 0x09 => ReplaceIfUnmodifiedRequest
         case 0x0B => RemoveRequest
         case 0x0D => RemoveIfUnmodifiedRequest
         case 0x0F => ContainsKeyRequest
         case 0x11 => GetWithVersionRequest
         case 0x13 => ClearRequest
         case 0x15 => StatsRequest
         case 0x17 => PingRequest
         case 0x19 => BulkGetRequest
         case _ => throw new HotRodUnknownOperationException("Unknown operation: " + streamOp, messageId)
      }
      if (isTraceEnabled) trace("Operation code: {0} has been matched to {1}", streamOp, op)
      
      val cacheName = buffer.readString
      val flag = buffer.readUnsignedInt match {
         case 0 => NoFlag
         case 1 => ForceReturnPreviousValue
      }
      val clientIntelligence = buffer.readUnsignedByte
      val topologyId = buffer.readUnsignedInt
      //todo use these once transaction support is added
      val txId = buffer.readByte
      if (txId != 0) throw new UnsupportedOperationException("Transaction types other than 0 (NO_TX) is not supported at this stage.  Saw TX_ID of " + txId)

      new HotRodHeader(op, messageId, cacheName, flag, clientIntelligence, topologyId, this)
   }

   override def readKey(buffer: ChannelBuffer): ByteArrayKey = new ByteArrayKey(buffer.readRangedBytes)

   override def readParameters(header: HotRodHeader, buffer: ChannelBuffer): Option[RequestParameters] = {
      header.op match {
         case RemoveRequest => None
         case RemoveIfUnmodifiedRequest => Some(new RequestParameters(null, -1, -1, buffer.readLong))
         case ReplaceIfUnmodifiedRequest => {
            val lifespan = readLifespanOrMaxIdle(buffer)
            val maxIdle = readLifespanOrMaxIdle(buffer)
            val version = buffer.readLong
            Some(new RequestParameters(buffer.readRangedBytes, lifespan, maxIdle, version))
         }
         case _ => {
            val lifespan = readLifespanOrMaxIdle(buffer)
            val maxIdle = readLifespanOrMaxIdle(buffer)
            Some(new RequestParameters(buffer.readRangedBytes, lifespan, maxIdle, -1))
         }
      }
   }

   private def readLifespanOrMaxIdle(buffer: ChannelBuffer): Int = {
      val stream = buffer.readUnsignedInt
      if (stream <= 0) -1 else stream
   }

   override def createValue(params: RequestParameters, nextVersion: Long): CacheValue =
      new CacheValue(params.data, nextVersion)

   override def createSuccessResponse(header: HotRodHeader, prev: CacheValue): AnyRef =
      createResponse(header, toResponse(header.op), Success, prev)

   override def createNotExecutedResponse(header: HotRodHeader, prev: CacheValue): AnyRef =
      createResponse(header, toResponse(header.op), OperationNotExecuted, prev)

   override def createNotExistResponse(header: HotRodHeader): AnyRef =
      createResponse(header, toResponse(header.op), KeyDoesNotExist, null)

   private def createResponse(h: HotRodHeader, op: OperationResponse, st: OperationStatus, prev: CacheValue): AnyRef = {
      if (h.flag == ForceReturnPreviousValue)
         new ResponseWithPrevious(h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId,
            if (prev == null) None else Some(prev.data))
      else
         new Response(h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId)
   }

   override def createGetResponse(h: HotRodHeader, v: CacheValue, op: Enumeration#Value): AnyRef = {
      if (v != null && op == GetRequest)
         new GetResponse(h.messageId, h.cacheName, h.clientIntel, GetResponse, Success, h.topologyId, Some(v.data))
      else if (v != null && op == GetWithVersionRequest)
         new GetWithVersionResponse(h.messageId, h.cacheName, h.clientIntel, GetWithVersionResponse, Success,
            h.topologyId, Some(v.data), v.version)
      else if (op == GetRequest)
         new GetResponse(h.messageId, h.cacheName, h.clientIntel, GetResponse, KeyDoesNotExist, h.topologyId, None)
      else
         new GetWithVersionResponse(h.messageId, h.cacheName, h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
            h.topologyId, None, 0)
   }

   override def handleCustomRequest(h: HotRodHeader, buffer: ChannelBuffer, cache: Cache[ByteArrayKey, CacheValue]): AnyRef = {
      h.op match {
         case RemoveIfUnmodifiedRequest => {
            val k = readKey(buffer)
            val params = readParameters(h, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               if (prev.version == params.get.streamVersion) {
                  val removed = cache.remove(k, prev);
                  if (removed)
                     createResponse(h, RemoveIfUnmodifiedResponse, Success, prev)
                  else
                     createResponse(h, RemoveIfUnmodifiedResponse, OperationNotExecuted, prev)
               } else {
                  createResponse(h, RemoveIfUnmodifiedResponse, OperationNotExecuted, prev)
               }
            } else {
               createResponse(h, RemoveIfUnmodifiedResponse, KeyDoesNotExist, prev)
            }
         }
         case ContainsKeyRequest => {
            val k = readKey(buffer)
            if (cache.containsKey(k))
               new Response(h.messageId, h.cacheName, h.clientIntel, ContainsKeyResponse, Success, h.topologyId)
            else
               new Response(h.messageId, h.cacheName, h.clientIntel, ContainsKeyResponse, KeyDoesNotExist, h.topologyId)
         }
         case ClearRequest => {
            // Get an optimised cache in case we can make the operation more efficient
            getOptimizedCache(h, cache).clear
            new Response(h.messageId, h.cacheName, h.clientIntel, ClearResponse, Success, h.topologyId)
         }
         case PingRequest => new Response(h.messageId, h.cacheName, h.clientIntel, PingResponse, Success, h.topologyId)
         case BulkGetRequest => {
            val count = buffer.readUnsignedInt
            if (isTraceEnabled) trace("About to create bulk response, count = " + count)
            new BulkGetResponse(h.messageId, h.cacheName, h.clientIntel, BulkGetResponse, Success, h.topologyId, count)
         }
      }
   }

   override def createStatsResponse(h: HotRodHeader, cacheStats: Stats): AnyRef = {
      val stats = mutable.Map.empty[String, String]
      stats += ("timeSinceStart" -> cacheStats.getTimeSinceStart.toString)
      stats += ("currentNumberOfEntries" -> cacheStats.getCurrentNumberOfEntries.toString)
      stats += ("totalNumberOfEntries" -> cacheStats.getTotalNumberOfEntries.toString)
      stats += ("stores" -> cacheStats.getStores.toString)
      stats += ("retrievals" -> cacheStats.getRetrievals.toString)
      stats += ("hits" -> cacheStats.getHits.toString)
      stats += ("misses" -> cacheStats.getMisses.toString)
      stats += ("removeHits" -> cacheStats.getRemoveHits.toString)
      stats += ("removeMisses" -> cacheStats.getRemoveMisses.toString)
      new StatsResponse(h.messageId, h.cacheName, h.clientIntel, immutable.Map[String, String]() ++ stats, h.topologyId)
   }

   override def createErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      t match {
         case i: IOException =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, ParseError, h.topologyId, i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, OperationTimedOut, h.topologyId, t.toString)
         case t: Throwable =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, ServerError, h.topologyId, t.toString)
      }
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache[ByteArrayKey, CacheValue]): Cache[ByteArrayKey, CacheValue] = {
      if (c.getConfiguration.getCacheMode.isDistributed && h.flag != ForceReturnPreviousValue) {
         c.getAdvancedCache.withFlags(SKIP_REMOTE_LOOKUP)
      } else {
         c
      }
   }

   def toResponse(request: Enumeration#Value): OperationResponse = {
      request match {
         case PutRequest => PutResponse
         case GetRequest => GetResponse
         case PutIfAbsentRequest => PutIfAbsentResponse
         case ReplaceRequest => ReplaceResponse
         case ReplaceIfUnmodifiedRequest => ReplaceIfUnmodifiedResponse
         case RemoveRequest => RemoveResponse
         case RemoveIfUnmodifiedRequest => RemoveIfUnmodifiedResponse
         case ContainsKeyRequest => ContainsKeyResponse
         case GetWithVersionRequest => GetWithVersionResponse
         case ClearRequest => ClearResponse
         case StatsRequest => StatsResponse
         case PingRequest => PingResponse
         case BulkGetRequest => BulkGetResponse
      }
   }

}

object OperationResponse extends Enumeration {
   type OperationResponse = Enumeration#Value
   val PutResponse = Value(0x02)
   val GetResponse = Value(0x04)
   val PutIfAbsentResponse = Value(0x06)
   val ReplaceResponse = Value(0x08)
   val ReplaceIfUnmodifiedResponse = Value(0x0A)
   val RemoveResponse = Value(0x0C)
   val RemoveIfUnmodifiedResponse = Value(0x0E)
   val ContainsKeyResponse = Value(0x10)
   val GetWithVersionResponse = Value(0x12)
   val ClearResponse = Value(0x14)
   val StatsResponse = Value(0x16)
   val PingResponse = Value(0x18)
   val BulkGetResponse = Value(0x1A)
   val ErrorResponse = Value(0x50)
}

object ProtocolFlag extends Enumeration {
   type ProtocolFlag = Enumeration#Value
   val NoFlag = Value
   val ForceReturnPreviousValue = Value
}
