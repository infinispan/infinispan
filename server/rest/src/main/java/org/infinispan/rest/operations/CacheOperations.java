package org.infinispan.rest.operations;

import java.util.Date;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalInt;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheSet;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.InfinispanCacheAPIRequest;
import org.infinispan.rest.InfinispanCacheResponse;
import org.infinispan.rest.InfinispanErrorResponse;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.JSONConstants;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.operations.exceptions.NoKeyException;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.EntrySetFormatter;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST Operations implementation. All operations translate {@link InfinispanRequest} into {@link InfinispanResponse}.
 *
 * @author Sebastian Łaskawiec
 */
public class CacheOperations extends AbstractOperations {

   /**
    * Creates new instance of {@link CacheOperations}.
    *
    * @param configuration REST Server configuration.
    * @param cacheManager  Embedded Cache Manager for storing data.
    */
   public CacheOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      super(configuration, cacheManager);
   }

   /**
    * Implementation of HTTP GET request invoked on root context.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse getCacheValues(InfinispanCacheAPIRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         MediaType contentType = getMediaType(request);
         AdvancedCache<String, Object> cache = restCacheManager.getCache(cacheName, contentType);
         CacheSet<String> keys = cache.keySet();
         MediaType mediaType = getMediaType(request);
         Charset charset = request.getAcceptContentType()
               .map(Charset::fromMediaType)
               .orElse(Charset.UTF8);

         if (mediaType == null) {
            mediaType = MediaType.TEXT_PLAIN;
         }
         InfinispanCacheResponse response = InfinispanCacheResponse.inReplyTo(request);
         response.contentType(mediaType.toString());
         response.charset(charset);
         response.cacheControl(CacheControl.noCache());
         OutputPrinter outputPrinter = EntrySetFormatter.forMediaType(mediaType);
         response.contentAsBytes(outputPrinter.print(cacheName, keys, charset));
         return response;
      } catch (CacheException cacheException) {
         throw createResponseException(cacheException);
      }
   }

   /**
    * Implementation of HTTP GET and HTTP HEAD requests invoked with a key.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse getCacheValue(InfinispanCacheAPIRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();

         MediaType mediaType = getMediaType(request);

         String key = request.getKey().orElseThrow(NoKeyException::new);
         String cacheControl = request.getCacheControl().orElse("");
         boolean returnBody = request.getRawRequest().method() == HttpMethod.GET;
         CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key, mediaType);
         InfinispanCacheResponse response = InfinispanCacheResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, Object> ice = (InternalCacheEntry<String, Object>) entry;
            Date lastMod = CacheOperationsHelper.lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = CacheOperationsHelper.minFresh(cacheControl);
            if (CacheOperationsHelper.entryFreshEnough(expires, minFreshSeconds)) {
               Metadata meta = ice.getMetadata();
               String etag = calcETAG(ice.getValue());
               if (CacheOperationsHelper.ifNoneMatchMathesEtag(request.getEtagIfNoneMatch(), etag)) {
                  response.status(HttpResponseStatus.NOT_MODIFIED);
                  return response;
               }
               if (CacheOperationsHelper.ifMatchDoesntMatchEtag(request.getEtagIfMatch(), etag)) {
                  response.status(HttpResponseStatus.PRECONDITION_FAILED);
                  return response;
               }
               if (CacheOperationsHelper.ifUnmodifiedIsBeforeEntryModificationDate(request.getEtagIfUnmodifiedSince(), lastMod)) {
                  response.status(HttpResponseStatus.PRECONDITION_FAILED);
                  return response;
               }
               if (CacheOperationsHelper.ifModifiedIsAfterEntryModificationDate(request.getEtagIfModifiedSince(), lastMod)) {
                  response.status(HttpResponseStatus.NOT_MODIFIED);
                  return response;
               }
               Object value = ice.getValue();
               MediaType configuredMediaType = restCacheManager.getConfiguredMediaType(cacheName);
               writeValue(value, mediaType, configuredMediaType, response, returnBody);

               response.status(HttpResponseStatus.OK);
               response.lastModified(lastMod);
               response.etag(etag);
               response.cacheControl(CacheOperationsHelper.calcCacheControl(expires));
               response.expires(expires);
               response.timeToLive(meta.lifespan());
               response.maxIdle(meta.maxIdle());

               if (request.getExtended().isPresent() && CacheOperationsHelper.supportsExtendedHeaders(restServerConfiguration, request.getExtended().get())) {
                  response.clusterPrimaryOwner(restCacheManager.getPrimaryOwner(cacheName, key, mediaType));
                  response.clusterNodeName(restCacheManager.getNodeName());
                  response.clusterServerAddress(restCacheManager.getServerAddress());
               }
            }
         }
         return response;
      } catch (CacheException cacheException) {
         throw createResponseException(cacheException);
      }
   }

   private void writeValue(Object value, MediaType requested, MediaType configuredMediaType, InfinispanResponse response, boolean returnBody) {
      String returnType;
      if (value instanceof byte[]) {
         if (returnBody) response.contentAsBytes((byte[]) value);
         returnType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
      } else {
         if (returnBody) response.contentAsText(value.toString());
         returnType = MediaType.TEXT_PLAIN_TYPE;
      }
      if (requested != null) {
         response.contentType(requested.toString());
      } else if (configuredMediaType == null) {
         response.contentType(returnType);
      } else {
         response.contentType(configuredMediaType.toString());
      }
   }

   /**
    * Implementation of HTTP DELETE request invoked with a key.
    *
    * @param request {@link InfinispanRequest}   to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse deleteCacheValue(InfinispanCacheAPIRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         String key = request.getKey().orElseThrow(NoKeyException::new);
         Optional<Boolean> useAsync = request.getUseAsync();
         MediaType contentType = request.getContentType().map(MediaType::fromString).orElse(null);
         CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key, null);

         InfinispanResponse response = InfinispanCacheResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, Object> ice = (InternalCacheEntry<String, Object>) entry;
            String etag = calcETAG(ice.getValue());
            Optional<String> clientEtag = request.getEtagIfNoneMatch();
            if (clientEtag.map(t -> t.equals(etag)).orElse(true)) {
               response.status(HttpResponseStatus.OK);
               if (useAsync.isPresent() && useAsync.get()) {
                  restCacheManager.getCache(cacheName, contentType).removeAsync(key);
               } else {
                  restCacheManager.getCache(cacheName, null).remove(key);
               }
            } else {
               //ETags don't match, so preconditions failed
               response.status(HttpResponseStatus.PRECONDITION_FAILED);
            }
         }
         return response;
      } catch (CacheException cacheException) {
         throw createResponseException(cacheException);
      }
   }

   /**
    * Implementation of HTTP DELETE request invoked on root context.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @param depth
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse clearOrRemoveEntireCache(InfinispanCacheAPIRequest request, String depth) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         Optional<Boolean> useAsync = request.getUseAsync();
         InfinispanResponse response = InfinispanCacheResponse.inReplyTo(request);
         if (depth == null || JSONConstants.INFINITY_NOROOT.equals(depth)) {
            response.status(HttpResponseStatus.OK);
            if (useAsync.isPresent() && useAsync.get()) {
               restCacheManager.getCache(cacheName, null).clearAsync();
            } else {
               restCacheManager.getCache(cacheName, null).clear();
            }
         } else if (JSONConstants.INFINITY.equals(depth)) {

            restCacheManager.getInstance().administration().withFlags(adminFlags(request.getRawRequest())).removeCache(cacheName);
            response.status(HttpResponseStatus.NO_CONTENT);
         } else {
            response.status(HttpResponseStatus.BAD_REQUEST);
         }
         return response;
      } catch (CacheException cacheException) {
         throw createResponseException(cacheException);
      }
   }

   /**
    * Implementation of HTTP PUT and HTTP POST requests invoked with a key.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse putValueToCache(InfinispanCacheAPIRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();

         MediaType contentType = request.getContentType().map(MediaType::fromString).orElse(null);
         AdvancedCache<String, Object> cache = restCacheManager.getCache(cacheName, contentType);
         String key = request.getKey().orElseThrow(NoKeyException::new);

         if (HttpMethod.POST.equals(request.getRawRequest().method()) && cache.containsKey(key)) {
            return InfinispanErrorResponse.asError(request, HttpResponseStatus.CONFLICT, "An entry already exists");
         } else {
            InfinispanCacheResponse response = InfinispanCacheResponse.inReplyTo(request);
            Optional<Object> oldData = Optional.empty();
            byte[] data = request.data().orElseThrow(NoDataFoundException::new);
            CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key, true, contentType);
            if (entry instanceof InternalCacheEntry) {
               InternalCacheEntry ice = (InternalCacheEntry) entry;
               oldData = Optional.of(entry.getValue());
               Optional<String> clientEtag = request.getEtagIfNoneMatch();
               if (clientEtag.isPresent()) {
                  String etag = calcETAG(ice.getValue());
                  if (clientEtag.get().equals(etag)) {
                     //client's and our ETAG match. Nothing to do, an entry is cached on the client side...
                     response.status(HttpResponseStatus.NOT_MODIFIED);
                     return response;
                  }
               }
            }

            boolean useAsync = request.getUseAsync().orElse(false);
            Optional<Long> ttl = request.getTimeToLiveSeconds();
            Optional<Long> idle = request.getMaxIdleTimeSeconds();
            return putInCache(response, useAsync, cache, key, data, ttl, idle, oldData);
         }
      } catch (CacheException | IllegalStateException e) {
         throw createResponseException(e);
      }
   }

   private <V> String calcETAG(V value) {
      return String.valueOf(hashFunc.hash(value));
   }

   private InfinispanResponse putInCache(InfinispanCacheResponse response, boolean useAsync, AdvancedCache<String, Object> cache, String key,
                                         byte[] data, Optional<Long> ttl, Optional<Long> idleTime, Optional<Object> prevCond) {
      final Metadata metadata = CacheOperationsHelper.createMetadata(cache.getCacheConfiguration(), ttl, idleTime);
      if (prevCond.isPresent()) {
         boolean replaced = cache.replace(key, prevCond.get(), data, metadata);
         // If not replaced, simply send back that the precondition failed
         if (!replaced) {
            response.status(HttpResponseStatus.PRECONDITION_FAILED);
         }
      } else {
         if (useAsync) {
            cache.putAsync(key, data, metadata);
         } else {
            cache.put(key, data, metadata);
         }
      }
      response.etag(calcETAG(data));
      return response;
   }

   public InfinispanResponse createCache(InfinispanCacheAPIRequest request, String template) {
      String cacheName = request.getCacheName().get();
      InfinispanResponse response = InfinispanCacheResponse.inReplyTo(request);
      if (restCacheManager.getInstance().cacheExists(cacheName)) {
         response.status(HttpResponseStatus.METHOD_NOT_ALLOWED);
      } else {
         restCacheManager.getInstance().administration().withFlags(adminFlags(request.getRawRequest())).createCache(cacheName, template);
      }

      return response;
   }

   private EnumSet<CacheContainerAdmin.AdminFlag> adminFlags(FullHttpRequest request) {
      return CacheContainerAdmin.AdminFlag.fromString(request.headers().get("Admin-Flags"));
   }
}
