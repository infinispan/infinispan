package org.infinispan.server.hotrod

import org.infinispan.server.core.Operation._
import HotRodOperation._
import OperationResponse._
import OperationStatus._
import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.{ChannelBuffer}
import org.infinispan.server.core.{UnknownOperationException, RequestParameters, Logging, CacheValue}
import org.infinispan.Cache
import collection.mutable
import collection.immutable
import org.infinispan.stats.Stats

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class Decoder10(cacheManager: CacheManager) extends AbstractVersionedDecoder {

   type SuitableHeader = HotRodHeader

   override def readHeader(buffer: ChannelBuffer, messageId: Long): HotRodHeader = {
      val streamOp = buffer.readUnsignedByte
      val op = OperationResolver.resolve(streamOp)
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
      if (header.op != RemoveRequest) {
         val lifespan = {
            val streamLifespan = buffer.readUnsignedInt
            if (streamLifespan <= 0) -1 else streamLifespan
         }
         val maxIdle = {
            val streamMaxIdle = buffer.readUnsignedInt
            if (streamMaxIdle <= 0) -1 else streamMaxIdle
         }
         val version = header.op match {
            case ReplaceIfUnmodifiedRequest | RemoveIfUnmodifiedRequest => buffer.readLong
            case _ => -1
         }
         val data = buffer.readRangedBytes
         Some(new RequestParameters(data, lifespan, maxIdle, version, false))
      } else {
         None
      }
   }

   override def createValue(params: RequestParameters, nextVersion: Long): CacheValue = new CacheValue(params.data, nextVersion)

   override def sendPutResponse(messageId: Long): AnyRef = new Response(messageId, PutResponse, Success)

   override def sendGetResponse(messageId: Long, v: CacheValue, op: Enumeration#Value): AnyRef = {
      if (v != null && op == GetRequest)
         new GetResponse(messageId, GetResponse, Success, Some(v.data))
      else if (v != null && op == GetWithVersionRequest)
         new GetWithVersionResponse(messageId, GetWithVersionResponse, Success, Some(v.data), v.version)
      else if (op == GetRequest)
         new GetResponse(messageId, GetResponse, KeyDoesNotExist, None)
      else
         new GetWithVersionResponse(messageId, GetWithVersionResponse, KeyDoesNotExist, None, 0)
   }

   override def sendPutIfAbsentResponse(messageId: Long, prev: CacheValue): AnyRef = {
      if (prev == null)
         new Response(messageId, PutIfAbsentResponse, Success)
      else
         new Response(messageId, PutIfAbsentResponse, OperationNotExecuted)
   }

   def sendReplaceResponse(messageId: Long, prev: CacheValue): AnyRef = {
      if (prev != null)
         new Response(messageId, ReplaceResponse, Success)
      else
         new Response(messageId, ReplaceResponse, OperationNotExecuted)
   }

   override def sendReplaceIfUnmodifiedResponse(messageId: Long, v: Option[CacheValue],
                                                prev: Option[CacheValue]): AnyRef = {
      if (v != None && prev != None)
         new Response(messageId, ReplaceIfUnmodifiedResponse, Success)
      else if (v == None && prev != None)
         new Response(messageId, ReplaceIfUnmodifiedResponse, OperationNotExecuted)
      else
         new Response(messageId, ReplaceIfUnmodifiedResponse, KeyDoesNotExist)
   }

   override def sendRemoveResponse(messageId: Long, prev: CacheValue): AnyRef = {
      if (prev != null)
         new Response(messageId, ReplaceResponse, Success)
      else
         new Response(messageId, ReplaceResponse, KeyDoesNotExist)
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

   override def sendStatsResponse(header: HotRodHeader, stats: Stats): AnyRef = {
      null
//      val cacheStats = cache.getAdvancedCache.getStats
//      val stats = mutable.Map[String, String]
//      stats += ("timeSinceStart", cacheStats.getTimeSinceStart)
//      stats += ("currentNumberOfEntries", cacheStats.getCurrentNumberOfEntries)
//      stats += ("totalNumberOfEntries", cacheStats.getTotalNumberOfEntries)
//      stats += ("stores", cacheStats.getStores)
//      stats += ("retrievals", cacheStats.getRetrievals)
//      stats += ("hits", cacheStats.getHits)
//      stats += ("misses", cacheStats.getMisses)
//      stats += ("removeHits", cacheStats.getRemoveHits)
//      stats += ("removeMisses", cacheStats.getRemoveMisses)
//      stats += ("evictions", cacheStats.getEvictions)
//      new StatsResponse(header.messageId, immutable.Map ++ stats)
   }
}

object OperationResolver extends Logging {
   private val operations = Map[Int, Enumeration#Value](
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

   def resolve(streamOp: Short): Option[Enumeration#Value] = {
      val op = operations.get(streamOp)
      if (op == None)
         trace("Operation code: {0} was unmatched", streamOp)
      else
         trace("Operation code: {0} has been matched to {1}", streamOp, op)
      op
   }

}