package org.infinispan.rest.operations;

import java.util.Date;
import java.util.Optional;
import java.util.OptionalInt;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheSet;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.exceptions.NoCacheFoundException;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.operations.exceptions.NoKeyException;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.MediaType;
import org.infinispan.rest.operations.mime.MimeMetadata;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST Operations implementation. All operations translate {@link InfinispanRequest} into {@link InfinispanResponse}.
 *
 * @author Sebastian Łaskawiec
 */
public class CacheOperations {

   private static final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   private final RestCacheManager<Object> restCacheManager;
   private final RestServerConfiguration restServerConfiguration;

   /**
    * Creates new instance of {@link CacheOperations}.
    *
    * @param configuration REST Server configuration.
    * @param cacheManager Embedded Cache Manager for storing data.
    */
   public CacheOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      this.restServerConfiguration = configuration;
      this.restCacheManager = cacheManager;
   }

   /**
    * Implementation of HTTP GET request invoked on root context.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse getCacheValues(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         AdvancedCache<String, Object> cache = restCacheManager.getCache(cacheName);
         CacheSet<String> keys = cache.keySet();
         MediaType mediaType = getMediaType(request);
         Charset charset = request.getAcceptContentType()
               .map(m -> Charset.fromMediaType(m))
               .orElse(Charset.UTF8);

         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.contentType(mediaType.toString());
         response.charset(charset);
         response.cacheControl(CacheControl.noCache());
         response.contentAsBytes(mediaType.getOutputPrinter().print(cacheName, keys, charset));
         return response;
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   /**
    * Implementation of HTTP GET and HTTP HEAD requests invoked with a key.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse getCacheValue(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         String key = request.getKey().orElseThrow(NoKeyException::new);
         String cacheControl = request.getCacheControl().orElse("");
         boolean returnBody = request.getRawRequest().method() == HttpMethod.GET;
         CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key);
         MediaType mediaType = getMediaType(request);
         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, Object> ice = (InternalCacheEntry<String, Object>) entry;
            Date lastMod = CacheOperationsHelper.lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = CacheOperationsHelper.minFresh(cacheControl);
            if (CacheOperationsHelper.entryFreshEnough(expires, minFreshSeconds)) {
               Metadata meta = ice.getMetadata();
               String metadataContentType;
               //We need to this this nasty trick because of compatibility. We might find objects in cache with unknown
               //media type. And moreover we need to ignore original content type sent by the client :)
               if (meta instanceof MimeMetadata) {
                  metadataContentType = ((MimeMetadata) meta).contentType();
                  mediaType = MediaType.fromMediaTypeAsString(metadataContentType);
               } else {
                  metadataContentType = mediaType.toString();
               }
               String etag = calcETAG(ice.getValue(), metadataContentType);
               Charset charset = request.getAcceptContentType().map(m -> Charset.fromMediaType(metadataContentType)).orElse(Charset.UTF8);
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

               //We are checking this twice, the first one was needed to obtain proper media type and check if all
               //preconditions were met. If everything is ok, we can proceed to print out the response (which might be heavy).
               if (meta instanceof MimeMetadata) {
                  if (returnBody) {
                     Object valueFromCacheWithMetadata = ice.getValue();
                     if (valueFromCacheWithMetadata instanceof byte[]) {
                        // The content was inserted using POST method and has been already written into bytes.
                        // There is no transcoding here - just put it in the response as is.
                        response.contentAsBytes((byte[]) ice.getValue());
                     } else {
                        // The format is unknown. We can only relay on proper implementation of #toString() method.
                        response.contentAsText(ice.getValue().toString());
                     }
                  }
               } else {
                  if (returnBody) {
                     //we know the media type is there. Otherwise an exception would be thrown by #getMediaType(request)
                     response.contentAsBytes(mediaType.getOutputPrinter().print(ice.getValue(), charset));
                  }
               }

               response.status(HttpResponseStatus.OK);
               response.contentType(metadataContentType);
               if (mediaType != null && mediaType.needsCharset()) {
                  response.charset(charset);
               }
               response.lastModified(lastMod);
               response.etag(etag);
               response.cacheControl(CacheOperationsHelper.calcCacheControl(expires));
               response.expires(expires);
               response.timeToLive(meta.lifespan());
               response.maxIdle(meta.maxIdle());

               if (request.getExtended().isPresent() && CacheOperationsHelper.supportsExtendedHeaders(restServerConfiguration, request.getExtended().get())) {
                  response.clusterPrimaryOwner(restCacheManager.getPrimaryOwner(cacheName, key));
                  response.clusterNodeName(restCacheManager.getNodeName().toString());
                  response.clusterServerAddress(restCacheManager.getServerAddress());
               }
            }
         }
         return response;
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   private MediaType getMediaType(InfinispanRequest request) throws UnacceptableDataFormatException {
      if (request.getAcceptContentType().isPresent()) {
         return request.getAcceptContentType()
               .map(m -> MediaType.fromMediaTypeAsString(m))
               .orElseThrow(UnacceptableDataFormatException::new);
      }
      return MediaType.TEXT_PLAIN;
   }

   /**
    * Implementation of HTTP DELETE request invoked with a key.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse deleteCacheValue(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         String key = request.getKey().orElseThrow(NoKeyException::new);
         Optional<Boolean> useAsync = request.getUseAsync();
         CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key);

         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, Object> ice = (InternalCacheEntry<String, Object>) entry;
            Metadata meta = entry.getMetadata();
            if (meta instanceof MimeMetadata) {
               String etag = calcETAG(ice.getValue(), ((MimeMetadata) meta).contentType());
               Optional<String> clientEtag = request.getEtagIfNoneMatch();
               if (clientEtag.map(t -> t.equals(etag)).orElse(true)) {
                  response.status(HttpResponseStatus.OK);
                  if (useAsync.isPresent() && useAsync.get()) {
                     restCacheManager.getCache(cacheName).removeAsync(key);
                  } else {
                     restCacheManager.getCache(cacheName).remove(key);
                  }
               } else {
                  //ETags don't match, so preconditions failed
                  response.status(HttpResponseStatus.PRECONDITION_FAILED);
               }
            }
         }
         return response;
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   /**
    * Implementation of HTTP DELETE request invoked on root context.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse clearEntireCache(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         Optional<Boolean> useAsync = request.getUseAsync();

         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.status(HttpResponseStatus.OK);

         if (useAsync.isPresent() && useAsync.get()) {
            restCacheManager.getCache(cacheName).clearAsync();
         } else {
            restCacheManager.getCache(cacheName).clear();
         }

         return response;
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   /**
    * Implementation of HTTP PUT and HTTP POST requests invoked with a key.
    *
    * @param request {@link InfinispanRequest} to be processed.
    * @return InfinispanResponse which shall be sent to the client.
    * @throws RestResponseException Thrown in case of any non-critical processing errors.
    */
   public InfinispanResponse putValueToCache(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName().get();
         AdvancedCache<String, Object> cache = restCacheManager.getCache(cacheName);
         String key = request.getKey().orElseThrow(NoKeyException::new);

         if (HttpMethod.POST.equals(request.getRawRequest().method()) && cache.containsKey(key)) {
            return InfinispanResponse.asError(request, HttpResponseStatus.CONFLICT, "An entry already exists");
         } else {
            InfinispanResponse response = InfinispanResponse.inReplyTo(request);
            Optional<Object> oldData = Optional.empty();
            byte[] data = request.data().orElseThrow(NoDataFoundException::new);
            CacheEntry<String, Object> entry = restCacheManager.getInternalEntry(cacheName, key, true);
            if (entry instanceof InternalCacheEntry) {
               InternalCacheEntry ice = (InternalCacheEntry) entry;
               oldData = Optional.of(entry.getValue());
               Metadata meta = ice.getMetadata();
               if (meta instanceof MimeMetadata) {
                  // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                  Optional<String> clientEtag = request.getEtagIfNoneMatch();
                  if (clientEtag.isPresent()) {
                     String etag = calcETAG(ice.getValue(), ((MimeMetadata) meta).contentType());
                     if (clientEtag.get().equals(etag)) {
                        //client's and our ETAG match. Nothing to do, an entry is cached on the client side...
                        response.status(HttpResponseStatus.NOT_MODIFIED);
                        return response;
                     }
                  }
               }
            }

            boolean useAsync = request.getUseAsync().orElse(false);
            String dataType = request.getContentType().orElse("text/plain");
            Optional<Long> ttl = request.getTimeToLiveSeconds();
            Optional<Long> idle = request.getMaxIdleTimeSeconds();
            return putInCache(response, useAsync, cache, key, data, dataType, ttl, idle, oldData);
         }
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   private <V> String calcETAG(V value, String contentType) {
      return contentType + hashFunc.hash(value);
   }

   private InfinispanResponse putInCache(InfinispanResponse response, boolean useAsync, AdvancedCache<String, Object> cache, String key,
                                         byte[] data, String dataType, Optional<Long> ttl, Optional<Long> idleTime, Optional<Object> prevCond) {
      final Metadata metadata = CacheOperationsHelper.createMetadata(cache.getCacheConfiguration(), dataType, ttl, idleTime);
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
      response.etag(calcETAG(data, dataType));
      return response;
   }

   public InfinispanResponse createCache(InfinispanRequest infinispanRequest) {
      this.restServerConfiguration.adminOperationsHandler().runTask()
   }
}
