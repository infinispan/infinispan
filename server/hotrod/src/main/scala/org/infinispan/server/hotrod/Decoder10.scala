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
import org.infinispan.manager.EmbeddedCacheManager

/**
 * HotRod protocol decoder specific for specification version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class Decoder10(cacheManager: EmbeddedCacheManager) extends AbstractVersionedDecoder {
   import RequestResolver._
   import ResponseResolver._
   import OperationResponse._
   import ProtocolFlag._
   import HotRodServer._
   type SuitableHeader = HotRodHeader

   private lazy val isClustered: Boolean = cacheManager.getGlobalConfiguration.getTransportClass != null
   private lazy val topologyCache: Cache[String, TopologyView] =
      if (isClustered) cacheManager.getCache(TopologyCacheName) else null

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
      val topologyResponse = getTopologyResponse(h)
      if (h.flag == ForceReturnPreviousValue)
         new ResponseWithPrevious(h.messageId, h.cacheName, h.clientIntel, op, st, topologyResponse,
            if (prev == null) None else Some(prev.data))
      else
         new Response(h.messageId, h.cacheName, h.clientIntel, op, st, topologyResponse)
   }

   override def createGetResponse(h: HotRodHeader, v: CacheValue, op: Enumeration#Value): AnyRef = {
      val topologyResponse = getTopologyResponse(h)
      if (v != null && op == GetRequest)
         new GetResponse(h.messageId, h.cacheName, h.clientIntel, GetResponse, Success, topologyResponse, Some(v.data))
      else if (v != null && op == GetWithVersionRequest)
         new GetWithVersionResponse(h.messageId, h.cacheName, h.clientIntel, GetWithVersionResponse, Success,
            topologyResponse, Some(v.data), v.version)
      else if (op == GetRequest)
         new GetResponse(h.messageId, h.cacheName, h.clientIntel, GetResponse, KeyDoesNotExist, topologyResponse, None)
      else
         new GetWithVersionResponse(h.messageId, h.cacheName, h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
            topologyResponse, None, 0)
   }

   override def handleCustomRequest(h: HotRodHeader, buffer: ChannelBuffer, cache: Cache[CacheKey, CacheValue]): AnyRef = {
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
            val topologyResponse = getTopologyResponse(h)
            val k = readKey(buffer)
            if (cache.containsKey(k))
               new Response(h.messageId, h.cacheName, h.clientIntel, ContainsKeyResponse, Success, topologyResponse)
            else
               new Response(h.messageId, h.cacheName, h.clientIntel, ContainsKeyResponse, KeyDoesNotExist, topologyResponse)
         }
         case ClearRequest => {
            val topologyResponse = getTopologyResponse(h)
            // Get an optimised cache in case we can make the operation more efficient
            getOptimizedCache(h, cache).clear
            new Response(h.messageId, h.cacheName, h.clientIntel, ClearResponse, Success, topologyResponse)
         }
         case PingRequest => {
            val topologyResponse = getTopologyResponse(h)
            new Response(h.messageId, h.cacheName, h.clientIntel, PingResponse, Success, topologyResponse)
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
      val topologyResponse = getTopologyResponse(h)
      new StatsResponse(h.messageId, h.cacheName, h.clientIntel, immutable.Map[String, String]() ++ stats, topologyResponse)
   }

   override def createErrorResponse(h: HotRodHeader, t: Throwable): AnyRef = {
      t match {
         case i: IOException =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, ParseError, getTopologyResponse(h), i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, OperationTimedOut, getTopologyResponse(h), t.toString)
         case t: Throwable =>
            new ErrorResponse(h.messageId, h.cacheName, h.clientIntel, ServerError, getTopologyResponse(h), t.toString)
      }
   }

   private def getTopologyResponse(h: HotRodHeader): Option[AbstractTopologyResponse] = {
      // If clustered, set up a cache for topology information
      if (isClustered) {
         h.clientIntel match {
            case 2 | 3 => {
               val currentTopologyView = topologyCache.get("view")
               if (h.topologyId != currentTopologyView.topologyId) {
                  val cache = cacheManager.getCache(h.cacheName)
                  val config = cache.getConfiguration
                  if (h.clientIntel == 2 || !config.getCacheMode.isDistributed) {
                     Some(TopologyAwareResponse(TopologyView(currentTopologyView.topologyId, currentTopologyView.members)))
                  } else { // Must be 3 and distributed
                     // TODO: Retrieve hash function when we have specified functions
                     val hashSpace = cache.getAdvancedCache.getDistributionManager.getConsistentHash.getHashSpace
                     Some(HashDistAwareResponse(TopologyView(currentTopologyView.topologyId, currentTopologyView.members),
                           config.getNumOwners, 1, hashSpace))
                  }
               } else None
            }
            case 1 => None
         }
      } else None
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache[CacheKey, CacheValue]): Cache[CacheKey, CacheValue] = {
      if (c.getConfiguration.getCacheMode.isDistributed && h.flag == ForceReturnPreviousValue) {
         c.getAdvancedCache.withFlags(SKIP_REMOTE_LOOKUP)
      } else {
         c
      }
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
         if (isTraceEnabled) trace("Operation code: {0} was unmatched", streamOp)
      else
         if (isTraceEnabled) trace("Operation code: {0} has been matched to {1}", streamOp, op)
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

object ProtocolFlag extends Enumeration {
   type ProtocolFlag = Enumeration#Value
   val NoFlag = Value(0)
   val ForceReturnPreviousValue = Value(1)
}