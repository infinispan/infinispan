package org.infinispan.server.hotrod

import javax.net.ssl.SSLPeerUnverifiedException
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import java.io.IOException
import org.infinispan.IllegalLifecycleStateException
import org.infinispan.commons.CacheException
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.context.Flag.{SKIP_CACHE_LOAD, SKIP_INDEXING, IGNORE_RETURN_VALUES}
import org.infinispan.remoting.transport.jgroups.SuspectException
import org.infinispan.server.core.Operation._
import org.infinispan.server.core._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport.NettyTransport
import org.infinispan.server.hotrod.HotRodOperation._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.stats.Stats
import org.infinispan.util.concurrent.TimeoutException
import scala.annotation.switch
import scala.collection.JavaConverters._
import javax.security.sasl.Sasl
import io.netty.channel.ChannelHandlerContext
import javax.security.auth.Subject
import java.security.PrivilegedAction
import javax.security.sasl.SaslServer
import io.netty.handler.ssl.SslHandler
import java.util.ArrayList
import java.security.Principal
import org.infinispan.server.core.security.InetAddressPrincipal
import java.net.InetSocketAddress
import org.infinispan.server.core.security.simple.SimpleUserPrincipal
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.infinispan.server.core.transport.SaslQopHandler
import org.infinispan.scripting.ScriptingManager
import javax.script.SimpleBindings
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import java.util.{BitSet => JavaBitSet}

import java.util.HashSet
import java.util.HashMap

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
      val streamOp = buffer.readUnsignedByte
      val (op, endOfOp) = (streamOp: @switch) match {
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
         case 0x21 => (AuthMechListRequest, true)
         case 0x23 => (AuthRequest, true)
         case 0x25 => (AddClientListenerRequest, false)
         case 0x27 => (RemoveClientListenerRequest, false)
         case 0x29 => (SizeRequest, true)
         case 0x2B => (ExecRequest, true)
         case 0x2D => (PutAllRequest, false)
         case 0x2F => (GetAllRequest, false)
         case 0x31 => (IterationStartRequest, false)
         case 0x33 => (IterationNextRequest, false)
         case 0x35 => (IterationEndRequest, false)
         case _ => throw new HotRodUnknownOperationException(
            "Unknown operation: " + streamOp, version, messageId)
      }
      if (isTrace) trace("Operation code: %d has been matched to %s", streamOp, op)

      val cacheName = readString(buffer)
      val flag = readUnsignedInt(buffer)
      val clientIntelligence = buffer.readUnsignedByte
      val topologyId = readUnsignedInt(buffer)

      header.op = op
      header.version = version
      header.messageId = messageId
      header.cacheName = cacheName
      header.flag = flag
      header.clientIntel = clientIntelligence
      header.topologyId = topologyId
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
         case RemoveIfUnmodifiedRequest => (new RequestParameters(-1, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), buffer.readLong), true)
         case ReplaceIfUnmodifiedRequest =>
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val version = buffer.readLong
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, version), false)
         case PutAllRequest =>
            // Since we have custom code handling for valueLength to allocate an array
            // always we have to pass back false and set the checkpoint manually...
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, -1), true)
         case GetAllRequest =>
            val count = readUnsignedInt(buffer)
            (new RequestParameters(count, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), -1), true)
         case _ =>
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, -1), false)
      }
   }

   private def hasFlag(h: HotRodHeader, f: ProtocolFlag): Boolean = {
      (h.flag & f.id) == f.id
   }

   private def readLifespanMaxIdle(buffer: ByteBuf, usingDefaultLifespan: Boolean, usingDefaultMaxIdle: Boolean, version: Byte): (ExpirationParam, ExpirationParam) = {
      def readDuration(useDefault: Boolean) = {
         val duration = readUnsignedInt(buffer)
         if (duration <= 0) {
            if (useDefault) EXPIRATION_DEFAULT else EXPIRATION_NONE
         } else duration
      }
      def readDurationIfNeeded(timeUnitValue: TimeUnitValue) = {
         if (timeUnitValue.isDefault) EXPIRATION_DEFAULT.toLong
         else {
            if (timeUnitValue.isInfinite) EXPIRATION_NONE.toLong else readUnsignedLong(buffer)
         }
      }
      version match {
         case VERSION_22 | VERSION_23 =>
            val timeUnits = TimeUnitValue.decodePair(buffer.readByte())
            val lifespanDuration = readDurationIfNeeded(timeUnits._1)
            val maxIdleDuration = readDurationIfNeeded(timeUnits._2)
            (new ExpirationParam(lifespanDuration, timeUnits._1), new ExpirationParam(maxIdleDuration, timeUnits._2))
         case _ =>
            val lifespan = readDuration(usingDefaultLifespan)
            val maxIdle = readDuration(usingDefaultMaxIdle)
            (new ExpirationParam(lifespan, TimeUnitValue.SECONDS), new ExpirationParam(maxIdle, TimeUnitValue.SECONDS))
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
            case (PutRequest, Success) => SuccessWithPrevious
            case (PutIfAbsentRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (ReplaceRequest, Success) => SuccessWithPrevious
            case (ReplaceIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (ReplaceIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (RemoveRequest, Success) => SuccessWithPrevious
            case (RemoveIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (RemoveIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case _ => st
         }

         adjustedStatus match {
            case SuccessWithPrevious | NotExecutedWithPrevious =>
               new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                  h.clientIntel, op, adjustedStatus, h.topologyId, if (prev == null) None else Some(prev))
            case _ =>
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, adjustedStatus, h.topologyId)
         }

      }
      else
         new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId)
   }

   override def createGetResponse(h: HotRodHeader, entry: CacheEntry[Array[Byte], Array[Byte]]): Response = {
      val op = h.op
      if (entry != null && op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            GetResponse, Success, h.topologyId,
            Some(entry.getValue))
      else if (entry != null && op == GetWithVersionRequest) {
         val version = entry.getMetadata.version().asInstanceOf[NumericVersion].getVersion
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, Success, h.topologyId,
            Some(entry.getValue), version)
      } else if (op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            GetResponse, KeyDoesNotExist, h.topologyId, None)
      else
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
            h.topologyId, None, 0)
   }

   override def customReadHeader(h: HotRodHeader, buffer: ByteBuf, cache: Cache,
       server: HotRodServer, ctx: ChannelHandlerContext): AnyRef = {
      h.op match {
         case ClearRequest =>
            // Get an optimised cache in case we can make the operation more efficient
            cache.clear()
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
               ClearResponse, Success, h.topologyId)
         case PingRequest => new Response(h.version, h.messageId, h.cacheName,
            h.clientIntel, PingResponse, Success, h.topologyId)
         case AuthMechListRequest =>
            new AuthMechListResponse(h.version, h.messageId, h.cacheName, h.clientIntel, server.getConfiguration.authentication.allowedMechs.asScala.toSet, h.topologyId)
         case AuthRequest =>
            if (!server.getConfiguration.authentication.enabled) {
               createErrorResponse(h, log.invalidOperation)
            } else {
               val decoder = ctx.pipeline.get("decoder").asInstanceOf[HotRodDecoder]
               val mech = readString(buffer)
               if (decoder.saslServer == null) {
                  val authConf = server.getConfiguration.authentication
                  val sap = authConf.serverAuthenticationProvider
                  val mechProperties = new HashMap[String, String](server.getConfiguration.authentication.mechProperties)
                  decoder.callbackHandler = sap.getCallbackHandler(mech, mechProperties)
                  val ssf = server.getSaslServerFactory(mech)
                  decoder.saslServer = if (authConf.serverSubject != null) {
                     Subject.doAs(authConf.serverSubject, new PrivilegedAction[SaslServer] {
                        def run : SaslServer = {
                           ssf.createSaslServer(mech, "hotrod",
                              server.getConfiguration.authentication.serverName,
                              mechProperties,
                              decoder.callbackHandler)
                         }
                     })
                  } else {
                     ssf.createSaslServer(mech, "hotrod",
                        server.getConfiguration.authentication.serverName,
                        mechProperties,
                        decoder.callbackHandler)
                  }
               }
               val clientResponse = readRangedBytes(buffer)
               val serverChallenge = decoder.saslServer.evaluateResponse(clientResponse)
               if (decoder.saslServer.isComplete) {
                  ctx.channel.writeAndFlush(new AuthResponse(h.version, h.messageId, h.cacheName, h.clientIntel, serverChallenge, h.topologyId))
                  val extraPrincipals = new ArrayList[Principal]
                  val id = normalizeAuthorizationId(decoder.saslServer.getAuthorizationID)
                  extraPrincipals.add(new SimpleUserPrincipal(id))
                  extraPrincipals.add(new InetAddressPrincipal(ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress].getAddress))
                  val sslHandler = ctx.pipeline.get("ssl").asInstanceOf[SslHandler]
                  try {
                     if (sslHandler != null) extraPrincipals.add(sslHandler.engine.getSession.getPeerPrincipal)
                  } catch { // Ignore any SSLPeerUnverifiedExceptions
                     case e: SSLPeerUnverifiedException => // ignore
                  }
                  decoder.subject = decoder.callbackHandler.getSubjectUserInfo(extraPrincipals).getSubject
                  val qop = decoder.saslServer.getNegotiatedProperty(Sasl.QOP).asInstanceOf[String]
                  if (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"))) {
                     val qopHandler = new SaslQopHandler(decoder.saslServer)
                     ctx.pipeline.addBefore("decoder", "saslQop", qopHandler)
                  } else {
                     decoder.saslServer.dispose()
                     decoder.callbackHandler = null
                     decoder.saslServer = null
                  }
                  None
               } else {
                  new AuthResponse(h.version, h.messageId, h.cacheName, h.clientIntel, serverChallenge, h.topologyId)
               }
            }
         case SizeRequest =>
            val size = cache.size()
            new SizeResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, size)
         case ExecRequest =>
            val marshaller = Option(server.getMarshaller).getOrElse(new GenericJBossMarshaller)
            val name = readString(buffer)
            val paramCount = readUnsignedInt(buffer)
            val params = new HashMap[String, Object]
            for (i <- 0 until paramCount) {
               val paramName = readString(buffer)
               val paramValue = marshaller.objectFromByteBuffer(readRangedBytes(buffer))
               params.put(paramName, paramValue)
            }
            params.put("marshaller", marshaller)
            params.put("cache", cache)
            val scriptingManager = SecurityActions.getCacheGlobalComponentRegistry(cache).getComponent(classOf[ScriptingManager])
            scriptingManager.setMarshaller(marshaller)
            val result: Any = scriptingManager.runScript(name, cache, new SimpleBindings(params)).get
            new ExecResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, marshaller.objectToByteBuffer(result))
      }
   }

   override def customReadKey(decoder: HotRodDecoder, h: HotRodHeader, buffer: ByteBuf,
       cache: Cache, server: HotRodServer, ch: Channel): AnyRef = {
      h.op match {
         case RemoveIfUnmodifiedRequest =>
            val k = readKey(buffer)
            val params = readParameters(h, buffer)._1
            val entry = cache.getCacheEntry(k)
            if (entry != null) {
               // Hacky, but CacheEntry has not been generified
               val prev = entry.getValue
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
         case ContainsKeyRequest =>
            val k = readKey(buffer)
            if (cache.containsKey(k))
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
                  ContainsKeyResponse, Success, h.topologyId)
            else
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
                  ContainsKeyResponse, KeyDoesNotExist, h.topologyId)
         case BulkGetRequest =>
            val count = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk response, count = %d", count)
            new BulkGetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               BulkGetResponse, Success, h.topologyId, count)
         case BulkGetKeysRequest =>
            val scope = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk get keys response, scope = %d", scope)
            new BulkGetKeysResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, BulkGetKeysResponse, Success, h.topologyId, scope)
         case GetWithMetadataRequest =>
            val k = readKey(buffer)
            getKeyMetadata(h, k, cache)
         case QueryRequest =>
            val query = readRangedBytes(buffer)
            val result = server.getQueryFacades.head.query(cache, query)
            new QueryResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, result)
         case AddClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val includeState = buffer.readByte() == 1
            val filterFactoryInfo = readNamedFactory(buffer)
            val converterFactoryInfo = readNamedFactory(buffer)
            val useRawData = h.version match {
               case VERSION_21 | VERSION_22 | VERSION_23 => buffer.readByte() == 1
               case _ => false
            }
            val reg = server.getClientListenerRegistry
            reg.addClientListener(ch, h, listenerId, cache, includeState,
                  (filterFactoryInfo, converterFactoryInfo), useRawData)
            createSuccessResponse(h, null)
         case RemoveClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val reg = server.getClientListenerRegistry
            val removed = reg.removeClientListener(listenerId, cache)
            if (removed)
               createSuccessResponse(h, null)
            else
               createNotExecutedResponse(h, null)
         case PutAllRequest | GetAllRequest =>
            decoder.checkpointTo(HotRodDecoderState.DECODE_PARAMETERS)
         case IterationStartRequest =>
            val segments = readOptRangedBytes(buffer)
            val filterConverterFactory = readOptString(buffer)
            val batchSize = readUnsignedInt(buffer)
            val iterationId = server.iterationManager.start(cache.getName, segments.map(JavaBitSet.valueOf), filterConverterFactory, batchSize)
            new IterationStartResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, iterationId)
         case IterationNextRequest =>
            val iterationId = readString(buffer)
            val iterationResult = server.iterationManager.next(cache.getName, iterationId)
            new IterationNextResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, iterationResult)
         case IterationEndRequest =>
            val iterationId = readString(buffer)
            val removed = server.iterationManager.close(cache.getName, iterationId)
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel, IterationEndResponse, if (removed) Success else InvalidIteration, h.topologyId)
         case _ => null
      }
   }

   private def readNamedFactory(buffer: ByteBuf): NamedFactory = {
      for {
         factoryName <- readOptionalString(buffer)
      } yield (factoryName, readOptionalParams(buffer))
   }

   private def readOptionalString(buffer: ByteBuf): Option[String] = {
      val string = readString(buffer)
      if (string.isEmpty) None else Some(string)
   }

   private def readOptionalParams(buffer: ByteBuf): List[Bytes] = {
      val numParams = buffer.readByte()
      if (numParams > 0) {
         var params = ListBuffer[Bytes]()
         for (i <- 0 until numParams) params += readRangedBytes(buffer)
         params.toList
      } else List.empty
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

   override def customReadValue(decoder: HotRodDecoder, h: HotRodHeader,
       hrCtx: CacheDecodeContext, buffer: ByteBuf, cache: Cache): AnyRef = {
      h.op match {
         case PutAllRequest =>
            var map = hrCtx.putAllMap
            if (map == null) {
              map = new HashMap[Bytes, Bytes]
              hrCtx.putAllMap = map
            }
            for (i <- map.size until hrCtx.params.valueLength) {
              val k = readRangedBytes(buffer)
              val v = readRangedBytes(buffer)
              map.put(k, v)
              // We check point after each read entry
              decoder.checkpoint
            }
            cache.putAll(map, hrCtx.buildMetadata)
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
               PutAllResponse, Success, h.topologyId)
         case GetAllRequest =>
           var set = hrCtx.getAllSet
           if (set == null) {
             set = new HashSet[Bytes]
             hrCtx.getAllSet = set
           }
           for (i <- set.size until hrCtx.params.valueLength) {
             val key = readRangedBytes(buffer)
             set.add(key)
             decoder.checkpoint
           }
           val results = cache.getAll(set)
           new GetAllResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               GetAllResponse, Success, h.topologyId, immutable.Map[Bytes, Bytes]() ++ results.asScala)
        case _ => null
     }
   }

   override def createStatsResponse(h: HotRodHeader, cacheStats: Stats, t: NettyTransport): StatsResponse = {
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

   override def getOptimizedCache(h: HotRodHeader, c: Cache): Cache = {
      val cacheCfg = SecurityActions.getCacheConfiguration(c)
      val isTransactional = cacheCfg.transaction().transactionMode().isTransactional
      val isClustered = cacheCfg.clustering().cacheMode().isClustered

      var optCache = c
      h.op match {
         case PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest
              | RemoveIfUnmodifiedRequest if isClustered && !isTransactional =>
            warnConditionalOperationNonTransactional(h.op.toString)
         case _ => // no-op
      }

      if (hasFlag(h, SkipCacheLoader)) {
         h.op match {
            case PutRequest
                 | GetRequest
                 | GetWithVersionRequest
                 | RemoveRequest
                 | ContainsKeyRequest
                 | BulkGetRequest
                 | GetWithMetadataRequest
                 | BulkGetKeysRequest =>
               optCache = optCache.withFlags(SKIP_CACHE_LOAD)
            case _ =>
         }
      }
      if (hasFlag(h, SkipIndexing)) {
         h.op match {
            case PutRequest
                 | PutIfAbsentRequest
                 | RemoveRequest
                 | RemoveIfUnmodifiedRequest
                 | ReplaceRequest
                 | ReplaceIfUnmodifiedRequest =>
               optCache = optCache.withFlags(SKIP_INDEXING)
            case _ =>
         }
      }
      if (!hasFlag(h, ForceReturnPreviousValue)) {
         h.op match {
            case PutRequest
                 | PutIfAbsentRequest =>
               optCache = optCache.withFlags(IGNORE_RETURN_VALUES)
            case _ =>
         }
      } else {
         h.op match {
            case PutRequest | RemoveRequest | PutIfAbsentRequest | ReplaceRequest
                 | ReplaceIfUnmodifiedRequest | RemoveIfUnmodifiedRequest if !isTransactional =>
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
      if (h.version == VERSION_22 | h.version == VERSION_23) {
         if (param.duration > 0) {
            val javaTimeUnit = param.unit.toJavaTimeUnit(h)
            javaTimeUnit.toMillis(param.duration)
         } else {
            param.duration
         }
      }
      else super.toMillis(param, h)
   }
}
