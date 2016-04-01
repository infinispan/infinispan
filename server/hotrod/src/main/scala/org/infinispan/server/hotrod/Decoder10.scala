package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.configuration.cache.Configuration
import OperationStatus._
import org.infinispan.server.core._
import collection.mutable
import collection.immutable
import org.infinispan.util.concurrent.TimeoutException
import java.io.IOException
import org.infinispan.context.Flag._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport.NettyTransport
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.NumericVersion
import io.netty.buffer.ByteBuf

import scala.annotation.switch

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
      if (header.op == null) {
         val part1 = for {
            streamOp <- readMaybeByte(buffer)
            cacheName <- readMaybeString(buffer)
         } yield {
            header.op = (streamOp: @switch) match {
               case 0x01 => HotRodOperation.PutRequest
               case 0x03 => HotRodOperation.GetRequest
               case 0x05 => HotRodOperation.PutIfAbsentRequest
               case 0x07 => HotRodOperation.ReplaceRequest
               case 0x09 => HotRodOperation.ReplaceIfUnmodifiedRequest
               case 0x0B => HotRodOperation.RemoveRequest
               case 0x0D => HotRodOperation.RemoveIfUnmodifiedRequest
               case 0x0F => HotRodOperation.ContainsKeyRequest
               case 0x11 => HotRodOperation.GetWithVersionRequest
               case 0x13 => HotRodOperation.ClearRequest
               case 0x15 => HotRodOperation.StatsRequest
               case 0x17 => HotRodOperation.PingRequest
               case 0x19 => HotRodOperation.BulkGetRequest
               case 0x1B => HotRodOperation.GetWithMetadataRequest
               case 0x1D => HotRodOperation.BulkGetKeysRequest
               case 0x1F => HotRodOperation.QueryRequest
               case _ => throw new HotRodUnknownOperationException(
                  "Unknown operation: " + streamOp, version, messageId)
            }
            if (isTrace) trace("Operation code: %d has been matched to %s", streamOp, header.op)
            header.cacheName = cacheName

            // Mark that we read up to here
            buffer.markReaderIndex()
         }
         if (part1.isEmpty) {
            return false
         }
      }

      val part2 = for {
         flag <- readMaybeVInt(buffer)
         clientIntelligence <- readMaybeByte(buffer)
         topologyId <- readMaybeVInt(buffer)
         txId <- readMaybeByte(buffer)
      } yield {
         header.flag = flag
         header.clientIntel = clientIntelligence
         header.topologyId = topologyId
         if (txId != 0) throw new UnsupportedOperationException("Transaction types other than 0 (NO_TX) is not supported at this stage.  Saw TX_ID of " + txId)

         // Mark that we read up to here
         buffer.markReaderIndex()
      }

      part2.isDefined
   }

   override def readParameters(header: HotRodHeader, buffer: ByteBuf): Option[RequestParameters] = {
      header.op match {
         case HotRodOperation.RemoveRequest => Some(null)
         case HotRodOperation.RemoveIfUnmodifiedRequest =>
            readMaybeLong(buffer).map(l => {
               new RequestParameters(-1, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), l)
            })
         case HotRodOperation.ReplaceIfUnmodifiedRequest =>
            for {
               lifespan <- readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan))
               maxIdle <- readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle))
               version <- readMaybeLong(buffer)
               valueLength <- readMaybeVInt(buffer)
            } yield {
               new RequestParameters(valueLength, new ExpirationParam(lifespan, TimeUnitValue.SECONDS), new ExpirationParam(maxIdle, TimeUnitValue.SECONDS), version)
            }
         case _ =>
            for {
               lifespan <- readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan))
               maxIdle <- readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle))
               valueLength <- readMaybeVInt(buffer)
            } yield {
               new RequestParameters(valueLength, new ExpirationParam(lifespan, TimeUnitValue.SECONDS), new ExpirationParam(maxIdle, TimeUnitValue.SECONDS), -1)
            }
      }
   }

   private def hasFlag(h: HotRodHeader, f: ProtocolFlag): Boolean = {
      (h.flag & f.id) == f.id
   }

   private def readLifespanOrMaxIdle(buffer: ByteBuf, useDefault: Boolean): Option[Int] = {
      readMaybeVInt(buffer).map(stream => {
         if (stream <= 0) {
            if (useDefault)
               EXPIRATION_DEFAULT
            else
               EXPIRATION_NONE
         } else stream
      })
   }

   override def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), Success, prev)

   override def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), OperationNotExecuted, prev)

   override def createNotExistResponse(header: HotRodHeader): Response =
      createResponse(header, toResponse(header.op), KeyDoesNotExist, null)

   private def createResponse(h: HotRodHeader, op: OperationResponse, st: OperationStatus, prev: Array[Byte]): Response = {
      if (hasFlag(h, ForceReturnPreviousValue))
         new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
               h.clientIntel, op, st, h.topologyId, Option(prev))
      else
         new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId)
   }

   override def createGetResponse(h: HotRodHeader, entry: CacheEntry[Array[Byte], Array[Byte]]): Response = {
      val op = h.op
      if (entry != null && op == HotRodOperation.GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               GetResponse, Success, h.topologyId,
               Some(entry.getValue))
      else if (entry != null && op == HotRodOperation.GetWithVersionRequest) {
         val version = entry.getMetadata.version().asInstanceOf[NumericVersion].getVersion
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, Success, h.topologyId,
            Some(entry.getValue), version)
      } else if (op == HotRodOperation.GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                         GetResponse, KeyDoesNotExist, h.topologyId, None)
      else
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
               h.topologyId, None, 0)
   }

   override def customReadHeader(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                                 out: java.util.List[AnyRef]): Unit = { }

   override def customReadKey(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                              out: java.util.List[AnyRef]): Unit = {
      h.op match {
         case HotRodOperation.BulkGetRequest | HotRodOperation.BulkGetKeysRequest =>
            for {
               number <- readMaybeVInt(buffer)
            } yield {
               hrCtx.operationDecodeContext = number
               buffer.markReaderIndex()
               out.add(hrCtx)
            }
         case HotRodOperation.QueryRequest =>
            for {
               query <- readMaybeRangedBytes(buffer)
            } yield {
               hrCtx.operationDecodeContext = query
               buffer.markReaderIndex()
               out.add(hrCtx)
            }
         case _ =>
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

   override def customReadValue(header: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                                out: java.util.List[AnyRef]): Unit = { }

   override def createStatsResponse(hrCtx: CacheDecodeContext, t: NettyTransport): StatsResponse = {
      val h = hrCtx.header
      val cacheStats = hrCtx.cache.getStats
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

   override def getOptimizedCache(h: HotRodHeader, c: Cache, cacheCfg: Configuration): Cache = {
      var optCache = c
      if (!hasFlag(h, ForceReturnPreviousValue)) {
         h.op match {
            case HotRodOperation.PutRequest =>
            case HotRodOperation.PutIfAbsentRequest =>
               optCache = optCache.withFlags(IGNORE_RETURN_VALUES)
            case _ =>
         }
      }
      optCache
   }

}
