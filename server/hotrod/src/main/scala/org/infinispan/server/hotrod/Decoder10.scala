package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.server.core.Operation._
import HotRodOperation._
import OperationStatus._
import org.infinispan.stats.Stats
import org.infinispan.server.core._
import collection.mutable
import collection.immutable
import org.infinispan.util.concurrent.TimeoutException
import java.io.IOException
import org.infinispan.context.Flag._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import transport.NettyTransport
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.NumericVersion
import io.netty.buffer.ByteBuf
import scala.Some
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.Channel

/**
 * HotRod protocol decoder specific for specification version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object Decoder10 extends AbstractVersionedDecoder with ServerConstants with Log {
   import OperationResponse._
   import ProtocolFlag._
   type SuitableHeader = HotRodHeader
   private val isTrace = isTraceEnabled

   override def readHeader(buffer: ByteBuf, version: Byte, messageId: Long, header: HotRodHeader): Boolean = {
      val streamOp = buffer.readUnsignedByte
      val (op, endOfOp) = streamOp match {
         case 0x01 => (PutRequest, false)
         case 0x03 => (GetRequest, false)
         case 0x05 => (PutIfAbsentRequest, false)
         case 0x07 => (ReplaceRequest, false)
         case 0x09 => (ReplaceIfUnmodifiedRequest, false)
         case 0x0B => (RemoveRequest, false)
         case 0x0D => (RemoveIfUnmodifiedRequest, false)
         case 0x0F => (ContainsKeyRequest, false)
         case 0x11 => (GetWithVersionRequest, false)
         case 0x13 => (ClearRequest, true)
         case 0x15 => (StatsRequest, true)
         case 0x17 => (PingRequest, true)
         case 0x19 => (BulkGetRequest, false)
         case 0x1B => (GetWithMetadataRequest, false)
         case 0x1D => (BulkGetKeysRequest, false)
         case 0x1F => (QueryRequest, false)
         case _ => throw new HotRodUnknownOperationException(
               "Unknown operation: " + streamOp, version, messageId)
      }
      if (isTrace) trace("Operation code: %d has been matched to %s", streamOp, op)

      val cacheName = readString(buffer)
      val flag = readUnsignedInt(buffer)
      val clientIntelligence = buffer.readUnsignedByte
      val topologyId = readUnsignedInt(buffer)
      // TODO: Use these once transaction support is added
      val txId = buffer.readByte
      if (txId != 0) throw new UnsupportedOperationException("Transaction types other than 0 (NO_TX) is not supported at this stage.  Saw TX_ID of " + txId)

      header.op = op
      header.version = version
      header.messageId = messageId
      header.cacheName = cacheName
      header.flag = flag
      header.clientIntel = clientIntelligence
      header.topologyId = topologyId
      header.decoder = this
      endOfOp
   }

   override def readKey(h: HotRodHeader, buffer: ByteBuf): (Array[Byte], Boolean) = {
      val k = readKey(buffer)
      h.op match {
         case RemoveRequest => (k, true)
         case _ => (k, false)
      }
   }

   private def readKey(buffer: ByteBuf): Array[Byte] = readRangedBytes(buffer)

   override def readParameters(header: HotRodHeader, buffer: ByteBuf): (RequestParameters, Boolean) = {
      header.op match {
         case RemoveRequest => (null, true)
         case RemoveIfUnmodifiedRequest => (new RequestParameters(-1, -1, -1, buffer.readLong), true)
         case ReplaceIfUnmodifiedRequest => {
            val lifespan = readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan))
            val maxIdle = readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle))
            val version = buffer.readLong
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, lifespan, maxIdle, version), false)
         }
         case _ => {
            val lifespan = readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan))
            val maxIdle = readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle))
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, lifespan, maxIdle, -1), false)
         }
      }
   }

   private def hasFlag(h: HotRodHeader, f: ProtocolFlag): Boolean = {
      (h.flag & f.id) == f.id
   }

   private def readLifespanOrMaxIdle(buffer: ByteBuf, useDefault: Boolean): Int = {
      val stream = readUnsignedInt(buffer)
      if (stream <= 0) {
         if (useDefault)
            EXPIRATION_DEFAULT
         else
            EXPIRATION_NONE
      } else stream
   }

   override def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): AnyRef =
      createResponse(header, toResponse(header.op), Success, prev)

   override def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): AnyRef =
      createResponse(header, toResponse(header.op), OperationNotExecuted, prev)

   override def createNotExistResponse(header: HotRodHeader): AnyRef =
      createResponse(header, toResponse(header.op), KeyDoesNotExist, null)

   private def createResponse(h: HotRodHeader, op: OperationResponse, st: OperationStatus, prev: Array[Byte]): AnyRef = {
      if (hasFlag(h, ForceReturnPreviousValue))
         new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
               h.clientIntel, op, st, h.topologyId, if (prev == null) None else Some(prev))
      else
         new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId)
   }

   override def createGetResponse(h: HotRodHeader, entry: CacheEntry[Array[Byte], Array[Byte]]): AnyRef = {
      val op = h.op
      if (entry != null && op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               GetResponse, Success, h.topologyId,
               Some(entry.getValue.asInstanceOf[Array[Byte]]))
      else if (entry != null && op == GetWithVersionRequest) {
         val version = entry.getMetadata.version().asInstanceOf[NumericVersion].getVersion
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, Success, h.topologyId,
            Some(entry.getValue.asInstanceOf[Array[Byte]]), version)
      } else if (op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                         GetResponse, KeyDoesNotExist, h.topologyId, None)
      else
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
               h.topologyId, None, 0)
   }

   override def customReadHeader(h: HotRodHeader, buffer: ByteBuf, cache: Cache, server: HotRodServer, ctx: ChannelHandlerContext): AnyRef = {
      h.op match {
         case ClearRequest => {
            // Get an optimised cache in case we can make the operation more efficient
            cache.clear()
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
                         ClearResponse, Success, h.topologyId)
         }
         case PingRequest => new Response(h.version, h.messageId, h.cacheName,
                  h.clientIntel, PingResponse, Success, h.topologyId)
      }
   }

   override def customReadKey(h: HotRodHeader, buffer: ByteBuf, cache: Cache, server: HotRodServer, ch: Channel): AnyRef = {
      h.op match {
         case RemoveIfUnmodifiedRequest => {
            val k = readKey(buffer)
            val params = readParameters(h, buffer)._1
            val entry = cache.getCacheEntry(k)
            if (entry != null) {
               // Hacky, but CacheEntry has not been generified
               val prev = entry.getValue.asInstanceOf[Array[Byte]]
               val streamVersion = new NumericVersion(params.streamVersion)
               if (entry.getMetadata.version() == streamVersion) {
                  val removed = cache.remove(k, prev)
                  if (removed)
                     createResponse(h, RemoveIfUnmodifiedResponse, Success, prev)
                  else
                     createResponse(h, RemoveIfUnmodifiedResponse, OperationNotExecuted, prev)
               } else {
                  createResponse(h, RemoveIfUnmodifiedResponse, OperationNotExecuted, prev)
               }
            } else {
               createResponse(h, RemoveIfUnmodifiedResponse, KeyDoesNotExist, null)
            }
         }
         case ContainsKeyRequest => {
            val k = readKey(buffer)
            if (cache.containsKey(k))
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
                            ContainsKeyResponse, Success, h.topologyId)
            else
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
                            ContainsKeyResponse, KeyDoesNotExist, h.topologyId)
         }
         case BulkGetRequest => {
            val count = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk response, count = %d", count)
            new BulkGetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                                BulkGetResponse, Success, h.topologyId, count)
         }
         case BulkGetKeysRequest => {
            val scope = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk get keys response, scope = %d", scope)
            new BulkGetKeysResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, BulkGetKeysResponse, Success, h.topologyId, scope)
         }
         case GetWithMetadataRequest => {
            val k = readKey(buffer)
            getKeyMetadata(h, k, cache)
         }
         case QueryRequest => {
            val query = readRangedBytes(buffer)
            val result = server.getQueryFacades.head.query(cache, query)
            new QueryResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, result)
         }
      }
   }

   def getKeyMetadata(h: HotRodHeader, k: Array[Byte], cache: Cache): GetWithMetadataResponse = {
      val ce = cache.getAdvancedCache.getCacheEntry(k)
      if (ce != null) {
         val ice = ce.asInstanceOf[InternalCacheEntry]
         val entryVersion = ice.getMetadata.version().asInstanceOf[NumericVersion]
         val v = ce.getValue
         val lifespan = if (ice.getLifespan < 0) -1 else (ice.getLifespan / 1000).toInt
         val maxIdle = if (ice.getMaxIdle < 0) -1 else (ice.getMaxIdle / 1000).toInt
         new GetWithMetadataResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, GetWithMetadataResponse, Success, h.topologyId,
                  Some(v), entryVersion.getVersion, ice.getCreated, lifespan, ice.getLastUsed, maxIdle)
      } else {
         new GetWithMetadataResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, GetWithMetadataResponse, KeyDoesNotExist, h.topologyId,
                  None, 0, -1, -1, -1, -1)
      }
   }

   override def customReadValue(header: HotRodHeader, buffer: ByteBuf, cache: Cache): AnyRef = null

   override def createStatsResponse(h: HotRodHeader, cacheStats: Stats, t: NettyTransport): AnyRef = {
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
      stats += ("totalBytesRead" -> t.getTotalBytesRead)
      stats += ("totalBytesWritten" -> t.getTotalBytesWritten)
      new StatsResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                        immutable.Map[String, String]() ++ stats, h.topologyId)
   }

   override def createErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      t match {
         case i: IOException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                              ParseError, h.topologyId, i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                              OperationTimedOut, h.topologyId, t.toString)
         case t: Throwable =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                              ServerError, h.topologyId, t.toString)
      }
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache): Cache = {
      var optCache = c
      if (!hasFlag(h, ForceReturnPreviousValue)) {
         h.op match {
            case PutRequest =>
            case PutIfAbsentRequest =>
               optCache = optCache.withFlags(IGNORE_RETURN_VALUES)
            case _ =>
         }
      }
      optCache
   }

}
