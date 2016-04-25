package org.infinispan.server.hotrod

import java.util.{Map, Set}

import org.infinispan.AdvancedCache
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.{EntryVersion, NumericVersion, NumericVersionGenerator, VersionGenerator}
import org.infinispan.context.Flag
import org.infinispan.factories.ComponentRegistry
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.{EmbeddedMetadata, Metadata}
import org.infinispan.registry.InternalCacheRegistry
import org.infinispan.remoting.rpc.RpcManager
import org.infinispan.server.core.ServerConstants
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log

/**
 * Invokes operations against the cache based on the state kept during decoding process
 *
 */
class CacheDecodeContext(server: HotRodServer) extends ServerConstants with Log {

   type BytesResponse = Bytes => Response

   val isTrace = isTraceEnabled

   @scala.beans.BeanProperty
   var error: Throwable = _
   @scala.beans.BeanProperty
   var decoder: AbstractVersionedDecoder = _
   @scala.beans.BeanProperty
   var header: HotRodHeader = _
   var cache: AdvancedCache[Bytes, Bytes] = _
   var key: Bytes = _
   var rawValue: Bytes = _
   var params: RequestParameters = _
   var putAllMap: Map[Bytes, Bytes] = _
   var getAllSet: Set[Bytes] = _
   var operationDecodeContext: Any = _

    def createExceptionResponse(e: Throwable): (ErrorResponse) = {
      e match {
         case i: InvalidMagicIdException =>
            logExceptionReported(i)
            new ErrorResponse(0, 0, "", 1, InvalidMagicOrMsgId, 0, i.toString)
         case e: HotRodUnknownOperationException =>
            logExceptionReported(e)
            new ErrorResponse(e.version, e.messageId, "", 1, UnknownOperation, 0, e.toString)
         case u: UnknownVersionException =>
            logExceptionReported(u)
            new ErrorResponse(u.version, u.messageId, "", 1, UnknownVersion, 0, u.toString)
         case r: RequestParsingException =>
            logExceptionReported(r)
            val msg =
               if (r.getCause == null)
                  r.toString
               else
                  "%s: %s".format(r.getMessage, r.getCause.toString)
            new ErrorResponse(r.version, r.messageId, "", 1, ParseError, 0, msg)
         case i: IllegalStateException =>
            // Some internal server code could throw this, so make sure it's logged
            logExceptionReported(i)
            decoder.createErrorResponse(header, i)
         case t: Throwable if decoder != null => decoder.createErrorResponse(header, t)
         case t: Throwable =>
            logErrorBeforeReadingRequest(t)
            new ErrorResponse(0, 0, "", 1, ServerError, 1, t.toString)
      }
   }

   def replace: Response = {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      var prev = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get(key)
      if (prev != null) {
         // Generate new version only if key present
         prev = cache.replace(key, rawValue, buildMetadata)
      }
      if (prev != null)
         successResp(prev)
      else
         notExecutedResp(prev)
   }

   def obtainCache(cacheManager: EmbeddedCacheManager, loopback: Boolean) = {
      val cacheName = header.cacheName
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      var cache = server.getKnownCacheInstance(cacheName)
      if (cache == null) {
         // Talking to the wrong cache are really request parsing errors
         // and hence should be treated as client errors
         val icr = cacheManager.getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
         if (icr.isPrivateCache(cacheName)) {
            throw new RequestParsingException(
               "Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'".format(cacheName),
               header.version, header.messageId)
         } else if (icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
            if (!cacheManager.getCacheManagerConfiguration.security.authorization.enabled && !loopback) {
               throw new RequestParsingException(
                  "Remote requests are allowed to protected caches only over loopback or if authorization is enabled. Do no send remote requests to cache '%s'".format(cacheName),
                  header.version, header.messageId)
            } else {
               // We want to make sure the cache access is checked everytime, so don't store it as a "known" cache. More
               // expensive, but these caches should not be accessed frequently
               cache = server.getCacheInstance(cacheName, cacheManager, skipCacheCheck = true, addToKnownCaches = false)
            }
         } else if (!cacheName.isEmpty && !(cacheManager.getCacheNames contains cacheName)) {
            throw new CacheNotFoundException(
               "Cache with name '%s' not found amongst the configured caches".format(cacheName),
               header.version, header.messageId)
         } else {
            cache = server.getCacheInstance(cacheName, cacheManager, skipCacheCheck = true)
         }
      }
      this.cache = decoder.getOptimizedCache(header, cache, server.getCacheConfiguration(cacheName))
   }

   def buildMetadata: Metadata = {
      val metadata = new EmbeddedMetadata.Builder
      metadata.version(generateVersion(server.getCacheRegistry(header.cacheName), cache))
      (params.lifespan, params.maxIdle) match {
         case (ExpirationParam(EXPIRATION_DEFAULT,_), ExpirationParam(EXPIRATION_DEFAULT,_)) =>
            // Do nothing - default is merged in via embedded
         case (_, ExpirationParam(EXPIRATION_DEFAULT,_)) =>
            metadata.lifespan(decoder.toMillis(params.lifespan, header))
         case (ExpirationParam(EXPIRATION_DEFAULT, _), _) =>
            metadata.maxIdle(decoder.toMillis(params.maxIdle, header))
         case (_, _) =>
            metadata.lifespan(decoder.toMillis(params.lifespan, header))
            .maxIdle(decoder.toMillis(params.maxIdle, header))
      }
      metadata.build()
   }

   def get: Response = createGetResponse(cache.getCacheEntry(key))

   def getKeyMetadata: GetWithMetadataResponse = {
      val ce = cache.getCacheEntry(key)
      if (ce != null) {
         val ice = ce.asInstanceOf[InternalCacheEntry]
         val entryVersion = ice.getMetadata.version().asInstanceOf[NumericVersion]
         val v = ce.getValue
         val lifespan = if (ice.getLifespan < 0) -1 else (ice.getLifespan / 1000).toInt
         val maxIdle = if (ice.getMaxIdle < 0) -1 else (ice.getMaxIdle / 1000).toInt
         new GetWithMetadataResponse(header.version, header.messageId, header.cacheName,
            header.clientIntel, OperationResponse.GetWithMetadataResponse, Success, header.topologyId,
            Some(v), entryVersion.getVersion, ice.getCreated, lifespan, ice.getLastUsed, maxIdle)
      } else {
         new GetWithMetadataResponse(header.version, header.messageId, header.cacheName,
            header.clientIntel, OperationResponse.GetWithMetadataResponse, KeyDoesNotExist, header.topologyId,
            None, 0, -1, -1, -1, -1)
      }
   }

   def containsKey: Response = {
      if (cache.containsKey(key))
         successResp(null)
      else
         notExistResp
   }

   def replaceIfUnmodified: Response = {
      val entry = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntry(key)
      if (entry != null) {
         val prev = entry.getValue
         val streamVersion = new NumericVersion(params.streamVersion)
         if (entry.getMetadata.version() == streamVersion) {
            val v = rawValue
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            val replaced = cache.replace(key, prev, v, buildMetadata)
            if (replaced)
               successResp(prev)
            else
               notExecutedResp(prev)
         } else {
            notExecutedResp(prev)
         }
      } else notExistResp
   }

   def putIfAbsent: Response = {
      var prev = cache.get(key)
      if (prev == null) {
         // Generate new version only if key not present
         prev = cache.putIfAbsent(key, rawValue, buildMetadata)
      }
      if (prev == null)
         successResp(prev)
      else
         notExecutedResp(prev)
   }

   def put: Response = {
      // Get an optimised cache in case we can make the operation more efficient
      val prev = cache.put(key, rawValue, buildMetadata)
      successResp(prev)
   }

   def generateVersion(registry: ComponentRegistry, cache: org.infinispan.Cache[Bytes, Bytes]): EntryVersion = {
      val cacheVersionGenerator = registry.getVersionGenerator
      if (cacheVersionGenerator == null) {
         // It could be null, for example when not running in compatibility mode.
         // The reason for that is that if no other component depends on the
         // version generator, the factory does not get invoked.
         val newVersionGenerator = new NumericVersionGenerator()
         .clustered(registry.getComponent(classOf[RpcManager]) != null)
         registry.registerComponent(newVersionGenerator, classOf[VersionGenerator])
         newVersionGenerator.generateNew()
      } else {
         cacheVersionGenerator.generateNew()
      }
   }

   def remove: Response = {
      val prev = cache.remove(key)
      if (prev != null)
         successResp(prev)
      else
         notExistResp
   }

   def removeIfUnmodified: Response = {
      val entry = cache.getCacheEntry(key)
      if (entry != null) {
         // Hacky, but CacheEntry has not been generified
         val prev = entry.getValue
         val streamVersion = new NumericVersion(params.streamVersion)
         if (entry.getMetadata.version() == streamVersion) {
            val removed = cache.remove(key, prev)
            if (removed)
               successResp(prev)
            else
               notExecutedResp(prev)
         } else {
            notExecutedResp(prev)
         }
      } else {
         notExistResp
      }
   }

   def clear: Response = {
      cache.clear()
      successResp(null)
   }

   def successResp(prev: Bytes): Response = decoder.createSuccessResponse(header, prev)

   def notExecutedResp(prev: Bytes): Response = decoder.createNotExecutedResponse(header, prev)

   def notExistResp: Response = decoder.createNotExistResponse(header)

   def createGetResponse(entry: CacheEntry[Bytes, Bytes]): Response = decoder.createGetResponse(header, entry)

   def createMultiGetResponse(pairs: Map[Bytes, CacheEntry[Bytes, Bytes]]): AnyRef = null

   def getCacheRegistry(cacheName: String): ComponentRegistry =
      server.getCacheRegistry(cacheName)
}

case class ExpirationParam(duration: Long, unit: TimeUnitValue) {
   override def toString = {
      new StringBuilder().append("ExpirationParam").append("{")
      .append("value=").append(duration)
      .append(", unit=").append(unit)
      .append("}").toString()
   }
}

class RequestParameters(val valueLength: Int, val lifespan: ExpirationParam, val maxIdle: ExpirationParam, val streamVersion: Long) {
   override def toString = {
      new StringBuilder().append("RequestParameters").append("{")
      .append("valueLength=").append(valueLength)
      .append(", lifespan=").append(lifespan)
      .append(", maxIdle=").append(maxIdle)
      .append(", streamVersion=").append(streamVersion)
      .append("}").toString()
   }
}
