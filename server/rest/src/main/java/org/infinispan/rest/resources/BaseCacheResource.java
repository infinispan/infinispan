package org.infinispan.rest.resources;

import static org.infinispan.rest.RequestHeader.EXTENDED_HEADER;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;

import java.util.Date;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.operations.CacheOperationsHelper;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.operations.exceptions.NoKeyException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handle basic cache operations.
 *
 * @since 10.0
 */
public class BaseCacheResource {

   private static final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   final CacheResourceQueryAction queryAction;
   final InvocationHelper invocationHelper;

   public BaseCacheResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.queryAction = new CacheResourceQueryAction(invocationHelper);
   }

   CompletionStage<RestResponse> deleteCacheValue(RestRequest request) throws RestResponseException {
      String cacheName = request.variables().get("cacheName");

      Object key = request.variables().get("cacheKey");
      if (key == null) throw new NoKeyException();

      MediaType keyContentType = request.keyContentType();
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      AdvancedCache<Object, Object> cache = restCacheManager.getCache(cacheName, keyContentType, MediaType.MATCH_ALL, request);

      return restCacheManager.getPrivilegedInternalEntry(cache, key, true).thenCompose(entry -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<Object, Object> ice = (InternalCacheEntry<Object, Object>) entry;
            String etag = calcETAG(ice.getValue());
            String clientEtag = request.getEtagIfNoneMatchHeader();
            if (clientEtag == null || clientEtag.equals(etag)) {
               responseBuilder.status(HttpResponseStatus.NO_CONTENT);
               return restCacheManager.remove(cacheName, key, keyContentType, request).thenApply(v -> responseBuilder.build());
            } else {
               //ETags don't match, so preconditions failed
               responseBuilder.status(HttpResponseStatus.PRECONDITION_FAILED);
            }
         }
         return CompletableFuture.completedFuture(responseBuilder.build());
      });
   }

   CompletionStage<RestResponse> putValueToCache(RestRequest request) {

      String cacheName = request.variables().get("cacheName");

      MediaType contentType = request.contentType();
      MediaType keyContentType = request.keyContentType();
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      AdvancedCache<Object, Object> cache = restCacheManager.getCache(cacheName, keyContentType, contentType, request);
      Object key = request.variables().get("cacheKey");
      if (key == null) throw new NoKeyException();
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder().status(HttpResponseStatus.NO_CONTENT);

      ContentSource contents = request.contents();
      if (contents == null) throw new NoDataFoundException();
      Long ttl = request.getTimeToLiveSecondsHeader();
      Long idle = request.getMaxIdleTimeSecondsHeader();

      byte[] data = request.contents().rawContent();

      return restCacheManager.getPrivilegedInternalEntry(cache, key, true).thenCompose(entry -> {
         if (request.method() == POST && entry != null) {
            return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.CONFLICT).entity("An entry already exists").build());
         }
         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry ice = (InternalCacheEntry) entry;
            String etagNoneMatch = request.getEtagIfNoneMatchHeader();
            if (etagNoneMatch != null) {
               String etag = calcETAG(ice.getValue());
               if (etagNoneMatch.equals(etag)) {
                  //client's and our ETAG match. Nothing to do, an entry is cached on the client side...
                  responseBuilder.status(HttpResponseStatus.NOT_MODIFIED);
                  return CompletableFuture.completedFuture(responseBuilder.build());
               }
            }
         }
         return putInCache(responseBuilder, cache, key, data, ttl, idle);
      });
   }

   CompletionStage<RestResponse> clearEntireCache(RestRequest request) throws RestResponseException {
      String cacheName = request.variables().get("cacheName");

      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      responseBuilder.status(HttpResponseStatus.NO_CONTENT);

      Cache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      return cache.clearAsync().thenApply(v -> responseBuilder.build());
   }


   CompletionStage<RestResponse> getCacheValue(RestRequest request) throws RestResponseException {
      String cacheName = request.variables().get("cacheName");

      MediaType keyContentType = request.keyContentType();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      MediaType requestedMediaType = negotiateMediaType(cache, request);

      Object key = request.variables().get("cacheKey");
      if (key == null) throw new NoKeyException();

      String cacheControl = request.getCacheControlHeader();
      boolean returnBody = request.method() == GET;
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      return restCacheManager.getInternalEntry(cacheName, key, keyContentType, requestedMediaType, request).thenApply(entry -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<Object, Object> ice = (InternalCacheEntry<Object, Object>) entry;
            Long lastMod = CacheOperationsHelper.lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = CacheOperationsHelper.minFresh(cacheControl);
            if (CacheOperationsHelper.entryFreshEnough(expires, minFreshSeconds)) {
               Metadata meta = ice.getMetadata();
               String etag = calcETAG(ice.getValue());
               String ifNoneMatch = request.getEtagIfNoneMatchHeader();
               String ifMatch = request.getEtagIfMatchHeader();
               String ifUnmodifiedSince = request.getIfUnmodifiedSinceHeader();
               String ifModifiedSince = request.getIfModifiedSinceHeader();
               if (ifNoneMatch != null && ifNoneMatch.equals(etag)) {
                  return responseBuilder.status(HttpResponseStatus.NOT_MODIFIED).build();
               }
               if (ifMatch != null && !ifMatch.equals(etag)) {
                  return responseBuilder.status(HttpResponseStatus.PRECONDITION_FAILED).build();
               }
               if (DateUtils.ifUnmodifiedIsBeforeModificationDate(ifUnmodifiedSince, lastMod)) {
                  return responseBuilder.status(HttpResponseStatus.PRECONDITION_FAILED).build();
               }
               if (DateUtils.isNotModifiedSince(ifModifiedSince, lastMod)) {
                  return responseBuilder.status(HttpResponseStatus.NOT_MODIFIED).build();
               }
               Object value = ice.getValue();
               MediaType configuredMediaType = restCacheManager.getValueConfiguredFormat(cacheName, request);
               writeValue(value, requestedMediaType, configuredMediaType, responseBuilder, returnBody);

               responseBuilder.status(HttpResponseStatus.OK)
                     .lastModified(lastMod)
                     .eTag(etag)
                     .cacheControl(CacheOperationsHelper.calcCacheControl(expires))
                     .expires(expires)
                     .timeToLive(meta.lifespan())
                     .maxIdle(meta.maxIdle())
                     .created(ice.getCreated())
                     .lastUsed(ice.getLastUsed());

               List<String> extended = request.parameters().get(EXTENDED_HEADER.getValue());
               RestServerConfiguration restServerConfiguration = invocationHelper.getConfiguration();
               if (extended != null && extended.size() > 0 && CacheOperationsHelper.supportsExtendedHeaders(restServerConfiguration, extended.iterator().next())) {
                  responseBuilder.clusterPrimaryOwner(restCacheManager.getPrimaryOwner(cacheName, key, request))
                        .clusterBackupOwners(restCacheManager.getBackupOwners(cacheName, key, request))
                        .clusterNodeName(restCacheManager.getNodeName())
                        .clusterServerAddress(restCacheManager.getServerAddress());
               }
            }
         }
         return responseBuilder.build();
      });
   }

   private void writeValue(Object value, MediaType requested, MediaType configuredMediaType, NettyRestResponse.Builder
         responseBuilder, boolean returnBody) {
      MediaType responseContentType;

      if (!requested.matchesAll()) {
         responseContentType = requested;
      } else {
         if (configuredMediaType == null) {
            responseContentType = value instanceof byte[] ? MediaType.APPLICATION_OCTET_STREAM : MediaType.TEXT_PLAIN;
         } else {
            responseContentType = configuredMediaType;
         }
      }
      responseBuilder.contentType(responseContentType);

      if (returnBody) responseBuilder.entity(value);
   }

   private <V> String calcETAG(V value) {
      return String.valueOf(hashFunc.hash(value));
   }

   private CompletionStage<RestResponse> putInCache(NettyRestResponse.Builder responseBuilder,
                                                    AdvancedCache<Object, Object> cache, Object key, byte[] data, Long ttl,
                                                    Long idleTime) {
      Configuration config = SecurityActions.getCacheConfiguration(cache);
      final Metadata metadata = CacheOperationsHelper.createMetadata(config, ttl, idleTime);
      responseBuilder.header("etag", calcETAG(data));
      CompletionStage<Object> stage;
      // Indexing is still blocking - can be removed when https://issues.redhat.com/browse/ISPN-11731 is complete
      if (config.indexing().enabled()) {
         stage = CompletableFuture.supplyAsync(() -> cache.putAsync(key, data, metadata), invocationHelper.getExecutor())
               .thenCompose(Function.identity());
      } else {
         stage = cache.putAsync(key, data, metadata);
      }
      return stage.thenApply(o -> responseBuilder.build());
   }

}
