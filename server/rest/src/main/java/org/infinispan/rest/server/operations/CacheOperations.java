package org.infinispan.rest.server.operations;

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
import org.infinispan.rest.MimeMetadata;
import org.infinispan.rest.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.server.CacheControl;
import org.infinispan.rest.server.InfinispanRequest;
import org.infinispan.rest.server.InfinispanResponse;
import org.infinispan.rest.server.operations.exceptions.NoCacheFoundException;
import org.infinispan.rest.server.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.server.operations.exceptions.NoKeyException;
import org.infinispan.rest.server.RestResponseException;
import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.MediaType;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class CacheOperations {

   private static final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   private final RestCacheManager restCacheManager;
   private final RestServerConfiguration restServerConfiguration;

   public CacheOperations(RestServerConfiguration configuration, RestCacheManager cacheManager) {
      this.restServerConfiguration = configuration;
      this.restCacheManager = cacheManager;
   }

   public InfinispanResponse getCacheValues(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName();
         AdvancedCache<String, byte[]> cache = restCacheManager.getCache(cacheName);
         CacheSet<String> keys = cache.keySet();
         MediaType mediaType = request.getAcceptContentType()
               .map(m -> MediaType.fromMediaTypeAsString(m))
               .orElse(MediaType.TEXT_PLAIN);
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

   public InfinispanResponse getCacheValue(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName();
         String key = request.getKey().orElseThrow(NoKeyException::new);
         String cacheControl = request.getCacheControl().orElse("");
         boolean returnBody = request.getRawRequest().method() == HttpMethod.GET;
         CacheEntry<String, byte[]> entry = restCacheManager.getInternalEntry(cacheName, key);
         MediaType mediaType = request.getAcceptContentType()
               .map(m -> MediaType.fromMediaTypeAsString(m))
               .orElse(MediaType.TEXT_PLAIN);
         Charset charset = request.getAcceptContentType()
               .map(m -> Charset.fromMediaType(m))
               .orElse(Charset.UTF8);

         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, byte[]> ice = (InternalCacheEntry<String, byte[]>) entry;
            Date lastMod = CacheOperationsHelper.lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = CacheOperationsHelper.minFresh(cacheControl);
            if (CacheOperationsHelper.entryFreshEnough(expires, minFreshSeconds)) {
               Metadata meta = ice.getMetadata();
               Optional<String> clientEtag = request.getEtag();
               if (clientEtag.isPresent()) {
                  // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                  String metadataContentType = (meta instanceof MimeMetadata) ? ((MimeMetadata) meta).contentType() : "";
                  String etag = calcETAG(ice.getValue(), metadataContentType);
                  if (clientEtag.isPresent() && clientEtag.get().equals(etag)) {
                     //client's and our ETAG match. Nothing to do, an entry is cached on the client side...
                     response.status(HttpResponseStatus.NOT_MODIFIED);
                     return response;
                  }
               }

               response.status(HttpResponseStatus.OK);
               response.lastModified(lastMod);
               response.cacheControl(CacheOperationsHelper.calcCacheControl(expires));
               response.expires(expires);
               response.timeToLive(meta.lifespan());
               response.maxIdle(meta.maxIdle());

               if (request.getExtended().isPresent() && CacheOperationsHelper.supportsExtendedHeaders(restServerConfiguration, request.getExtended().get())) {
                  response.clusterPrimaryOwner(restCacheManager.getPrimaryOwner(cacheName, key));
                  response.clusterNodeName(restCacheManager.getNodeName().toString());
                  response.clusterServerAddress(restCacheManager.getServerAddress());
               }

               if (returnBody) {
                  String mediaTypeForResponse = mediaType.toString();
                  if (meta instanceof MimeMetadata) {
                     String contentTypeFromMetadata = ((MimeMetadata) meta).contentType();
                     mediaTypeForResponse = contentTypeFromMetadata;
                     charset = Charset.fromMediaType(contentTypeFromMetadata);
                     //the data in cache is already encoded in a specific format. We don't need any mapping or printing.
                     response.contentAsBytes(ice.getValue());
                  } else {
                     response.contentAsBytes(mediaType.getOutputPrinter().print(ice.getValue(), charset));
                  }
                  response.contentType(mediaTypeForResponse);
                  response.charset(charset);
                  response.etag(calcETAG(ice.getValue(), mediaType.toString()));
               }
            }
         }
         return response;
      } catch (CacheException cacheException) {
         throw new NoCacheFoundException(cacheException.getLocalizedMessage());
      }
   }

   public InfinispanResponse deleteCacheValue(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName();
         String key = request.getKey().orElseThrow(NoKeyException::new);
         Optional<Boolean> useAsync = request.getUseAsync();
         CacheEntry<String, byte[]> entry = restCacheManager.getInternalEntry(cacheName, key);

         InfinispanResponse response = InfinispanResponse.inReplyTo(request);
         response.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, byte[]> ice = (InternalCacheEntry<String, byte[]>) entry;
            Metadata meta = entry.getMetadata();
            if (meta instanceof MimeMetadata) {
               String etag = calcETAG(ice.getValue(), ((MimeMetadata) meta).contentType());
               Optional<String> clientEtag = request.getEtag();
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

   public InfinispanResponse clearEntireCache(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName();
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

   public InfinispanResponse putValueToCache(InfinispanRequest request) throws RestResponseException {
      try {
         String cacheName = request.getCacheName();
         AdvancedCache<String, byte[]> cache = restCacheManager.getCache(cacheName);
         String key = request.getKey().orElseThrow(NoKeyException::new);

         if (HttpMethod.POST.equals(request.getRawRequest().method()) && cache.containsKey(key)) {
            return InfinispanResponse.asError(HttpResponseStatus.CONFLICT, "An entry already exists");
         } else {
            InfinispanResponse response = InfinispanResponse.inReplyTo(request);
            Optional<byte[]> oldData = Optional.empty();
            byte[] data = request.data().orElseThrow(NoDataFoundException::new);
            CacheEntry<String, byte[]> entry = restCacheManager.getInternalEntry(cacheName, key, true);
            if (entry instanceof InternalCacheEntry) {
               InternalCacheEntry ice = (InternalCacheEntry) entry;
               oldData = Optional.of(entry.getValue());
               Optional<String> clientEtag = request.getEtag();
               Metadata meta = ice.getMetadata();
               if (meta instanceof MimeMetadata) {
                  // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                  String etag = calcETAG(ice.getValue(), ((MimeMetadata) meta).contentType());
                  if (clientEtag.isPresent() && clientEtag.get().equals(etag)) {
                     //client's and our ETAG match. Nothing to do, an entry is cached on the client side...
                     response.status(HttpResponseStatus.NOT_MODIFIED);
                     return response;
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

   private InfinispanResponse putInCache(InfinispanResponse response, boolean useAsync, AdvancedCache<String, byte[]> cache, String key,
                                         byte[] data, String dataType, Optional<Long> ttl, Optional<Long> idleTime, Optional<byte[]> prevCond) {
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

}
