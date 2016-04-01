package org.infinispan.server.hotrod

import java.io.IOException
import java.security.PrivilegedActionException
import java.util.{HashMap, HashSet, Map, BitSet => JavaBitSet}

import io.netty.buffer.ByteBuf
import org.infinispan.IllegalLifecycleStateException
import org.infinispan.commons.CacheException
import org.infinispan.configuration.cache.Configuration
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.context.Flag.{IGNORE_RETURN_VALUES, SKIP_CACHE_LOAD, SKIP_INDEXING}
import org.infinispan.remoting.transport.jgroups.SuspectException
import org.infinispan.server.core._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport.NettyTransport
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.stats.ClusterCacheStats
import org.infinispan.util.concurrent.TimeoutException

import scala.annotation.{switch, tailrec}
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
 * HotRod protocol decoder specific for specification version 2.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
object Decoder2x extends AbstractVersionedDecoder with ServerConstants with Log with Constants {

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
               case 0x21 => HotRodOperation.AuthMechListRequest
               case 0x23 => HotRodOperation.AuthRequest
               case 0x25 => HotRodOperation.AddClientListenerRequest
               case 0x27 => HotRodOperation.RemoveClientListenerRequest
               case 0x29 => HotRodOperation.SizeRequest
               case 0x2B => HotRodOperation.ExecRequest
               case 0x2D => HotRodOperation.PutAllRequest
               case 0x2F => HotRodOperation.GetAllRequest
               case 0x31 => HotRodOperation.IterationStartRequest
               case 0x33 => HotRodOperation.IterationNextRequest
               case 0x35 => HotRodOperation.IterationEndRequest
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
      } yield {
         header.flag = flag
         header.clientIntel = clientIntelligence
         header.topologyId = topologyId

         // Mark that we read up to here
         buffer.markReaderIndex()
      }

      part2.isDefined
   }

   override def readParameters(header: HotRodHeader, buffer: ByteBuf): Option[RequestParameters] = {
      header.op match {
         case HotRodOperation.RemoveRequest => Some(null)
         case HotRodOperation.RemoveIfUnmodifiedRequest =>
            readMaybeLong(buffer).map(v =>
               new RequestParameters(-1, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), v))
         case HotRodOperation.ReplaceIfUnmodifiedRequest =>
            for {
               expirationParams <- readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
               version <- readMaybeLong(buffer)
               valueLength <- readMaybeVInt(buffer)
            } yield new RequestParameters(valueLength, expirationParams._1, expirationParams._2, version)
         case HotRodOperation.GetAllRequest =>
            readMaybeVInt(buffer).map(i =>
               new RequestParameters(i, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), -1))
         case _ =>
            for {
               expirationParams <- readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
               valueLength <- readMaybeVInt(buffer)
            } yield new RequestParameters(valueLength, expirationParams._1, expirationParams._2, -1)
      }
   }

   private def hasFlag(h: HotRodHeader, f: ProtocolFlag): Boolean = {
      (h.flag & f.id) == f.id
   }

   private def readLifespanMaxIdle(buffer: ByteBuf, usingDefaultLifespan: Boolean, usingDefaultMaxIdle: Boolean, version: Byte): Option[(ExpirationParam, ExpirationParam)] = {
      def readDuration(useDefault: Boolean): Option[Int] = {
         readMaybeVInt(buffer).map(duration => {
            if (duration <= 0) {
               if (useDefault) EXPIRATION_DEFAULT else EXPIRATION_NONE
            } else duration
         })
      }
      def readDurationIfNeeded(timeUnitValue: TimeUnitValue): Option[Long] = {
         if (timeUnitValue.isDefault) Some(EXPIRATION_DEFAULT)
         else {
            if (timeUnitValue.isInfinite) Some(EXPIRATION_NONE) else readMaybeVLong(buffer)
         }
      }
      version match {
         case ver if Constants.isVersionPre22(ver) =>
            for {
               lifespan <- readDuration(usingDefaultLifespan)
               maxIdle <- readDuration(usingDefaultMaxIdle)
            } yield (new ExpirationParam(lifespan, TimeUnitValue.SECONDS), new ExpirationParam(maxIdle, TimeUnitValue.SECONDS))
         case _ => // from 2.2 onwards
            readMaybeByte(buffer).map(t => {
               val timeUnits = TimeUnitValue.decodePair(t)
               for {
                  lifespanDuration <- readDurationIfNeeded(timeUnits._1)
                  maxIdleDuration <- readDurationIfNeeded(timeUnits._2)
               } yield (new ExpirationParam(lifespanDuration, timeUnits._1), new ExpirationParam(maxIdleDuration, timeUnits._2))
            }).getOrElse(None)
      }
   }

   override def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), Success, prev)

   override def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), OperationNotExecuted, prev)

   override def createNotExistResponse(header: HotRodHeader): Response =
      createResponse(header, toResponse(header.op), KeyDoesNotExist, null)

   private def createResponse(h: HotRodHeader, op: OperationResponse, st: OperationStatus, prev: Array[Byte]): Response = {
      if (hasFlag(h, ForceReturnPreviousValue)) {
         val adjustedStatus = (h.op, st) match {
            case (HotRodOperation.PutRequest, Success) => SuccessWithPrevious
            case (HotRodOperation.PutIfAbsentRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (HotRodOperation.ReplaceRequest, Success) => SuccessWithPrevious
            case (HotRodOperation.ReplaceIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (HotRodOperation.ReplaceIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (HotRodOperation.RemoveRequest, Success) => SuccessWithPrevious
            case (HotRodOperation.RemoveIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (HotRodOperation.RemoveIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case _ => st
         }

         adjustedStatus match {
            case SuccessWithPrevious | NotExecutedWithPrevious =>
               new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                  h.clientIntel, op, adjustedStatus, h.topologyId, Option(prev))
            case _ =>
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, adjustedStatus, h.topologyId)
         }

      }
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
                                 out: java.util.List[AnyRef]): Unit = {
      h.op match {
         case HotRodOperation.AuthRequest =>
            for {
               mech <- readMaybeString(buffer)
               clientResponse <- readMaybeRangedBytes(buffer)
            } yield {
               hrCtx.operationDecodeContext = (mech, clientResponse)
               buffer.markReaderIndex()
               out.add(hrCtx)
            }
         case HotRodOperation.ExecRequest =>
            var execCtx = hrCtx.operationDecodeContext.asInstanceOf[ExecRequestContext]
            // first time read
            if (execCtx == null) {
               val part1 = for {
                  name <- readMaybeString(buffer)
                  paramCount <- readMaybeVInt(buffer)
               } yield {
                  execCtx = new ExecRequestContext(name, paramCount, new HashMap[String, Bytes](paramCount))
                  hrCtx.operationDecodeContext = execCtx
                  // Mark that we read these
                  buffer.markReaderIndex()
               }
               if (part1.isEmpty) {
                  return
               }
            }

            @tailrec def addEntry(map: Map[String, Bytes]): Boolean = {
               val complete = for {
                  key <- readMaybeString(buffer)
                  value <- readMaybeRangedBytes(buffer)
               } yield {
                  map.put(key, value)
                  // Mark after each key value pair since they are in the map now
                  buffer.markReaderIndex()
               }
               if (complete.isDefined) {
                  // If we are the same size as param size we are done, otherwise continue until we
                  // can't read anymore or finally get to size
                  if (map.size() < execCtx.paramSize) {
                     addEntry(map)
                  } else true
               } else false
            }
            // Now if we have no params or if we were able to add them all continue with the context
            if (execCtx.paramSize == 0 || addEntry(execCtx.params)) {
               out.add(hrCtx)
            }
         case _ =>
            // This operation doesn't need additional reads - has everything to process
            out.add(hrCtx)
      }
   }

   override def customReadKey(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                              out: java.util.List[AnyRef]): Unit = {
      h.op match {
         case HotRodOperation.BulkGetRequest | HotRodOperation.BulkGetKeysRequest =>
            readMaybeVInt(buffer).foreach(number => {
               hrCtx.operationDecodeContext = number
               buffer.markReaderIndex()
               out.add(hrCtx)
            })
         case HotRodOperation.QueryRequest =>
            readMaybeRangedBytes(buffer).foreach(query => {
               hrCtx.operationDecodeContext = query
               buffer.markReaderIndex()
               out.add(hrCtx)
            })
         case HotRodOperation.AddClientListenerRequest =>
            var execCtx = hrCtx.operationDecodeContext.asInstanceOf[ClientListenerRequestContext]
            if (execCtx == null) {
               val part1 = for {
                  listenerId <- readMaybeRangedBytes(buffer)
                  includeState <- readMaybeByte(buffer)
               } yield {
                  execCtx = new ClientListenerRequestContext(listenerId, includeState == 1)
                  hrCtx.operationDecodeContext = execCtx
                  // Mark that we read these
                  buffer.markReaderIndex()
               }
               if (part1.isEmpty) {
                  return
               }
            }
            if (execCtx.filterFactoryInfo == null) {
               if (!readMaybeNamedFactory(buffer).exists(f => {
                  execCtx.filterFactoryInfo = f
                  true
               })) {
                  return
               }
               buffer.markReaderIndex()
            }
            for {
               converter <- readMaybeNamedFactory(buffer)
               useRawData <- h.version match {
                   // TODO: is this version check needed? - this should always be 2x
                  case ver if Constants.isVersion2x(ver) => readMaybeByte(buffer).map(b => b == 1)
                  case _ => Some(false)
               }
            } yield {
               execCtx.converterFactoryInfo = converter
               execCtx.useRawData = useRawData

               buffer.markReaderIndex()
               out.add(hrCtx)
            }
         case HotRodOperation.RemoveClientListenerRequest =>
            readMaybeRangedBytes(buffer).foreach(listenerId => {
               hrCtx.operationDecodeContext = listenerId
               buffer.markReaderIndex()
               out.add(hrCtx)
            })
         case HotRodOperation.IterationStartRequest =>
            for {
               segments <- readMaybeOptRangedBytes(buffer) match {
                  case MoreBytesForBytes => None
                  case BytesNotPresent => Some(None)
                  case pb: PresentBytes => Some(Some(pb.getValue))
               }
               factory <- readMaybeOptString(buffer) match {
                  case MoreBytesForString => None
                  case StringNotPresent => Some(None)
                  case ps: PresentString => if (Constants.isVersionPre24(h.version)) Some(Some((ps.getValue, List.empty)))
                                            else readOptionalParams(buffer).map(p => Some((ps.getValue, p)))
               }
               batchSize <- readMaybeVInt(buffer)
               metadata <- if (Constants.isVersionPre24(h.version)) Some(false) else readMaybeByte(buffer).map(m => m != 0)
            } yield {
               hrCtx.operationDecodeContext = (segments, factory, batchSize, metadata)
               buffer.markReaderIndex()
               out.add(hrCtx)
            }
         case HotRodOperation.IterationNextRequest | HotRodOperation.IterationEndRequest =>
            readMaybeString(buffer).foreach(iterationId => {
               hrCtx.operationDecodeContext = iterationId
               buffer.markReaderIndex()
               out.add(hrCtx)
            })
         case _ =>
      }
   }

   private def readMaybeNamedFactory(buffer: ByteBuf): Option[NamedFactory] = {
      readMaybeString(buffer).flatMap(name => {
         if (!name.isEmpty) {
            readOptionalParams(buffer).map(param => Some(name, param))
         } else Some(None)
      })
   }

   private def readOptionalParams(buffer: ByteBuf): Option[List[Bytes]] = {
      val numParams = readMaybeByte(buffer)
      numParams.map(p => {
         if (p > 0) {
            val params = ListBuffer[Bytes]()
            @tailrec def readParams(buf: ListBuffer[Bytes]): Boolean = {
               if (readMaybeRangedBytes(buffer).exists(param => {
                  buf += param
                  true
               })) {
                  if (buf.length < p) {
                     readParams(buf)
                  } else true
               } else false
            }
            if (readParams(params)) Some(params.toList) else None
         } else Some(List.empty)
      }).getOrElse(None)
   }

   def getKeyMetadata(h: HotRodHeader, k: Array[Byte], cache: Cache): GetWithMetadataResponse = {
      val ce = cache.getCacheEntry(k)
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

   override def customReadValue(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                                out: java.util.List[AnyRef]): Unit = {
      h.op match {
         case HotRodOperation.PutAllRequest =>
            val maxLength =  hrCtx.params.valueLength
            var map = hrCtx.putAllMap
            if (map == null) {
              map = new HashMap[Bytes, Bytes](maxLength)
              hrCtx.putAllMap = map
            }
            @tailrec def addEntry(): Boolean = {
               val complete = for {
                  key <- readMaybeRangedBytes(buffer)
                  value <- readMaybeRangedBytes(buffer)
               } yield {
                  map.put(key, value)
                  // Mark after each key value pair since they are in the map now
                  buffer.markReaderIndex()
               }
               if (complete.isDefined) {
                  // If we are the same size as param size we are done, otherwise continue until we
                  // can't read anymore or finally get to size
                  if (map.size() < maxLength) {
                     addEntry()
                  } else true
               } else false
            }
            if (addEntry()) {
               out.add(hrCtx)
            }
         case HotRodOperation.GetAllRequest =>
            val maxLength =  hrCtx.params.valueLength
            var set = hrCtx.getAllSet
            if (set == null) {
               set = new HashSet[Bytes](maxLength)
               hrCtx.getAllSet = set
            }
            @tailrec def addItem(): Boolean = {
               if (readMaybeRangedBytes(buffer).exists(k => {
                  set.add(k)
                  buffer.markReaderIndex()
                  true
               })) {
                  if (set.size() < maxLength) {
                     addItem()
                  } else true
               } else false
            }
            if (addItem()) {
               out.add(hrCtx)
            }
         case _ =>
      }
   }

   override def createStatsResponse(ctx: CacheDecodeContext, t: NettyTransport): StatsResponse = {
      val cacheStats = ctx.cache.getStats
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


      val h = ctx.header
      if (!Constants.isVersionPre24(h.version)) {
         val registry = ctx.getCacheRegistry(h.cacheName)
         Option(registry.getComponent(classOf[ClusterCacheStats])).foreach(clusterCacheStats => {
            stats += ("globalCurrentNumberOfEntries" -> clusterCacheStats.getCurrentNumberOfEntries.toString)
            stats += ("globalStores" -> clusterCacheStats.getStores.toString)
            stats += ("globalRetrievals" -> clusterCacheStats.getRetrievals.toString)
            stats += ("globalHits" -> clusterCacheStats.getHits.toString)
            stats += ("globalMisses" -> clusterCacheStats.getMisses.toString)
            stats += ("globalRemoveHits" -> clusterCacheStats.getRemoveHits.toString)
            stats += ("globalRemoveMisses" -> clusterCacheStats.getRemoveMisses.toString)
         })
      }
      new StatsResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         immutable.Map[String, String]() ++ stats, h.topologyId)
   }

   override def createErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      t match {
         case _ : SuspectException => createNodeSuspectedErrorResponse(h, t)
         case e: IllegalLifecycleStateException => createIllegalLifecycleStateErrorResponse(h, t)
         case i: IOException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               ParseError, h.topologyId, i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationTimedOut, h.topologyId, t.toString)
         case c: CacheException => c.getCause match {
            // JGroups and remote exceptions (inside RemoteException) can come wrapped up
            case _ : org.jgroups.SuspectedException => createNodeSuspectedErrorResponse(h, t)
            case _ : IllegalLifecycleStateException => createIllegalLifecycleStateErrorResponse(h, t)
            case _ => createServerErrorResponse(h, t)
         }
         case p: PrivilegedActionException => createErrorResponse(h, p.getCause)
         case t: Throwable => createServerErrorResponse(h, t)
      }
   }

   private def createNodeSuspectedErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         NodeSuspected, h.topologyId, t.toString)
   }

   private def createIllegalLifecycleStateErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         IllegalLifecycleState, h.topologyId, t.toString)
   }

   private def createServerErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         ServerError, h.topologyId, createErrorMsg(t))
   }

   def createErrorMsg(t: Throwable): String = {
      val causes = mutable.LinkedHashSet[Throwable]()
      var initial = t
      while (initial != null && !causes.contains(initial)) {
         causes += initial
         initial = initial.getCause
      }
      causes.mkString("\n")
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache, cacheCfg: Configuration): Cache = {
      val isTransactional = cacheCfg.transaction().transactionMode().isTransactional
      val isClustered = cacheCfg.clustering().cacheMode().isClustered

      var optCache = c
      h.op match {
         case op if h.op.isConditional() && isClustered && !isTransactional =>
            warnConditionalOperationNonTransactional(h.op.toString)
         case _ => // no-op
      }

      if (hasFlag(h, SkipCacheLoader)) {
         h.op match {
            case op if h.op.canSkipCacheLoading() =>
               optCache = optCache.withFlags(SKIP_CACHE_LOAD)
            case _ =>
         }
      }
      if (hasFlag(h, SkipIndexing)) {
         h.op match {
            case op if h.op.canSkipIndexing() =>
               optCache = optCache.withFlags(SKIP_INDEXING)
            case _ =>
         }
      }
      if (!hasFlag(h, ForceReturnPreviousValue)) {
         h.op match {
            case op if h.op.isNotConditionalAndCanReturnPrevious() =>
               optCache = optCache.withFlags(IGNORE_RETURN_VALUES)
            case _ =>
         }
      } else {
         h.op match {
            case op if h.op.canReturnPreviousValue() && !isTransactional =>
               warnForceReturnPreviousNonTransactional(h.op.toString)
            case _ => // no-op
         }
      }
      optCache
   }

   def normalizeAuthorizationId(id: String): String = {
      val realm = id.indexOf('@')
      if (realm >= 0) id.substring(0, realm) else id
   }

   /**
    * Convert an expiration value into milliseconds
    */
   override def toMillis(param: ExpirationParam, h: SuitableHeader): Long = {
      if (Constants.isVersionPre22(h.version)) super.toMillis(param, h)
      else
         if (param.duration > 0) {
            val javaTimeUnit = param.unit.toJavaTimeUnit(h)
            javaTimeUnit.toMillis(param.duration)
         } else {
            param.duration
         }
   }
}

class ExecRequestContext(val name: String, val paramSize: Int, val params: Map[String, Bytes]) { }

class ClientListenerRequestContext(val listenerId: Bytes, val includeCurrentState: Boolean) {
   var filterFactoryInfo: NamedFactory = _
   var converterFactoryInfo: NamedFactory = _
   var useRawData: Boolean = _
}