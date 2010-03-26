package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._
import HotRodOperation._
import OperationStatus._
import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.{ChannelBuffer}
import org.infinispan.Cache
import org.infinispan.stats.Stats
import org.infinispan.server.core._
import collection.mutable
import collection.immutable

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class Decoder10(cacheManager: CacheManager) extends AbstractVersionedDecoder {
   import RequestResolver._
   import ResponseResolver._
   import OperationResponse._
   type SuitableHeader = HotRodHeader

   override def readHeader(buffer: ChannelBuffer, messageId: Long): HotRodHeader = {
      val streamOp = buffer.readUnsignedByte
      val op = toRequest(streamOp)
      if (op == None) {
         throw new UnknownOperationException("Unknown operation: " + streamOp);
      }
      val cacheName = buffer.readString
      val flag = ProtocolFlag.apply(buffer.readUnsignedInt)
      val clientIntelligence = buffer.readUnsignedByte
      val topologyId = buffer.readUnsignedInt
      new HotRodHeader(op.get, messageId, cacheName, flag, clientIntelligence, topologyId, this)
   }

   override def readKey(buffer: ChannelBuffer): CacheKey = new CacheKey(buffer.readRangedBytes)

   override def readKeys(buffer: ChannelBuffer): Array[CacheKey] = Array(new CacheKey(buffer.readRangedBytes))

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

   override def createSuccessResponse(header: HotRodHeader): AnyRef =
      new Response(header.messageId, toResponse(header.op), Success)

   override def createNotExecutedResponse(header: HotRodHeader): AnyRef =
      new Response(header.messageId, toResponse(header.op), OperationNotExecuted)

   override def createNotExistResponse(header: HotRodHeader): AnyRef =
      new Response(header.messageId, toResponse(header.op), KeyDoesNotExist)   

   override def createGetResponse(messageId: Long, v: CacheValue, op: Enumeration#Value): AnyRef = {
      if (v != null && op == GetRequest)
         new GetResponse(messageId, GetResponse, Success, Some(v.data))
      else if (v != null && op == GetWithVersionRequest)
         new GetWithVersionResponse(messageId, GetWithVersionResponse, Success, Some(v.data), v.version)
      else if (op == GetRequest)
         new GetResponse(messageId, GetResponse, KeyDoesNotExist, None)
      else
         new GetWithVersionResponse(messageId, GetWithVersionResponse, KeyDoesNotExist, None, 0)
   }

   override def handleCustomRequest(header: HotRodHeader, buffer: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef = {
      val messageId = header.messageId
      header.op match {
         case RemoveIfUnmodifiedRequest => {
            val k = readKey(buffer)
            val params = readParameters(header, buffer)
            val prev = cache.get(k)
            if (prev != null) {
               if (prev.version == params.get.streamVersion) {
                  val removed = cache.remove(k, prev);
                  if (removed)
                     new Response(messageId, RemoveIfUnmodifiedResponse, Success)
                  else
                     new Response(messageId, RemoveIfUnmodifiedResponse, OperationNotExecuted)
               } else {
                  new Response(messageId, RemoveIfUnmodifiedResponse, OperationNotExecuted)
               }
            } else {
               new Response(messageId, RemoveIfUnmodifiedResponse, KeyDoesNotExist)
            }
         }
         case ContainsKeyRequest => {
            val k = readKey(buffer)
            if (cache.containsKey(k))
               new Response(messageId, ContainsKeyResponse, Success)
            else
               new Response(messageId, ContainsKeyResponse, KeyDoesNotExist)
         }
         case ClearRequest => {
            cache.clear
            new Response(messageId, ClearResponse, Success)
         }
         case PingRequest => new Response(messageId, PingResponse, Success) 
      }
   }

   override def createStatsResponse(header: HotRodHeader, cacheStats: Stats): AnyRef = {
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
      new StatsResponse(header.messageId, immutable.Map[String, String]() ++ stats)
   }

}

object RequestResolver extends Logging {
   private val requests = Map[Int, Enumeration#Value](
      0x01 -> PutRequest,
      0x03 -> GetRequest,
      0x05 -> PutIfAbsentRequest,
      0x07 -> ReplaceRequest,
      0x09 -> ReplaceIfUnmodifiedRequest,
      0x0B -> RemoveRequest,
      0x0D -> RemoveIfUnmodifiedRequest,
      0x0F -> ContainsKeyRequest,
      0x11 -> GetWithVersionRequest,
      0x13 -> ClearRequest,
      0x15 -> StatsRequest,
      0x17 -> PingRequest 
   )

   def toRequest(streamOp: Short): Option[Enumeration#Value] = {
      val op = requests.get(streamOp)
      if (op == None)
         trace("Operation code: {0} was unmatched", streamOp)
      else
         trace("Operation code: {0} has been matched to {1}", streamOp, op)
      op
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
   val ErrorResponse = Value(0x50)
}

object ResponseResolver {
   import OperationResponse._
   private val responses = Map[Enumeration#Value, OperationResponse](
      PutRequest -> PutResponse,
      GetRequest -> GetResponse,
      PutIfAbsentRequest -> PutIfAbsentResponse,
      ReplaceRequest -> ReplaceResponse,
      ReplaceIfUnmodifiedRequest -> ReplaceIfUnmodifiedResponse,
      RemoveRequest -> RemoveResponse,
      RemoveIfUnmodifiedRequest -> RemoveIfUnmodifiedResponse,
      ContainsKeyRequest -> ContainsKeyResponse,
      GetWithVersionRequest -> GetWithVersionResponse,
      ClearRequest -> ClearResponse,
      StatsRequest -> StatsResponse,
      PingRequest -> PingResponse
   )

   def toResponse(request: Enumeration#Value): OperationResponse = {
      responses.get(request).get
   }
}