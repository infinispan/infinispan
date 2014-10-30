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
import org.infinispan.distexec.mapreduce._
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
import java.util.HashMap
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.infinispan.server.core.transport.SaslQopHandler

/**
 * HotRod protocol decoder specific for specification version 2.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
object Decoder2x extends AbstractVersionedDecoder with ServerConstants with Log {

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
         case AuthMechListRequest => {
            new AuthMechListResponse(h.version, h.messageId, h.cacheName, h.clientIntel, server.getConfiguration.authentication.allowedMechs.asScala.toSet, h.topologyId)
         }
         case AuthRequest => {
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
                  val qop = decoder.saslServer.getNegotiatedProperty(Sasl.QOP).asInstanceOf[String];
                  if (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"))) {
                     val qopHandler = new SaslQopHandler(decoder.saslServer)
                     ctx.pipeline.addBefore("decoder", "saslQop", qopHandler)
                  } else {
                     decoder.saslServer.dispose
                     decoder.callbackHandler = null
                     decoder.saslServer = null
                  }
                  None
               } else {
                  new AuthResponse(h.version, h.messageId, h.cacheName, h.clientIntel, serverChallenge, h.topologyId)
               }
            }
         }
         case SizeRequest =>
            val size = cache.size()
            new SizeResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, size)
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
         case AddClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val includeState = if (buffer.readByte() == 0) false else true
            val filterFactoryInfo = readNamedFactory(buffer)
            val converterFactoryInfo = readNamedFactory(buffer)
            val reg = server.getClientListenerRegistry
            reg.addClientListener(ch, h, listenerId, cache, includeState, filterFactoryInfo, converterFactoryInfo)
            createSuccessResponse(h, null)
         case RemoveClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val reg = server.getClientListenerRegistry
            val removed = reg.removeClientListener(listenerId, cache)
            if (removed)
               createSuccessResponse(h, null)
            else
               createNotExecutedResponse(h, null)
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
         case _ : SuspectException => createNodeSuspectedErrorResponse(h, t)
         case e: IllegalLifecycleStateException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               IllegalLifecycleState, h.topologyId, t.toString)
         case i: IOException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               ParseError, h.topologyId, i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationTimedOut, h.topologyId, t.toString)
         case c: CacheException => c.getCause match {
            // JGroups exceptions come wrapped up
            case _ : org.jgroups.SuspectedException => createNodeSuspectedErrorResponse(h, t)
            case _ => createServerErrorResponse(h, t)
         }
         case t: Throwable => createServerErrorResponse(h, t)
      }
   }

   private def createNodeSuspectedErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         NodeSuspected, h.topologyId, t.toString)
   }

   private def createServerErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         ServerError, h.topologyId, t.toString)
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache): Cache = {
      var optCache = c
      h.op match {
         case PutIfAbsentRequest | ReplaceRequest | ReplaceIfUnmodifiedRequest
              | RemoveIfUnmodifiedRequest if !isCacheTransactional(optCache) =>
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
         if (!isCacheTransactional(optCache))
            warnForceReturnPreviousNonTransactional(h.op.toString)
      }
      optCache
   }

   private def isCacheTransactional(c: Cache): Boolean =
      SecurityActions.getCacheConfiguration(c).transaction().transactionMode().isTransactional

   def normalizeAuthorizationId(id: String): String = {
      val realm = id.indexOf('@')
      if (realm >= 0) id.substring(0, realm) else id
   }

}
