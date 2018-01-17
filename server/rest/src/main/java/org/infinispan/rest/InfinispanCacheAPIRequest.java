package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.rest.operations.CacheOperations;
import org.infinispan.rest.operations.StaticContent;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;

/**
 * Representation of a HTTP request related to Cache API operations.
 *
 * @since 9.2
 */
public class InfinispanCacheAPIRequest extends InfinispanRequest {

   private final Optional<Object> key;
   private final CacheOperations cacheOperations;

   InfinispanCacheAPIRequest(CacheOperations operations, FullHttpRequest request, ChannelHandlerContext ctx, Optional<String> cacheName, Optional<Object> key, String context, Map<String, List<String>> parameters) {
      super(request, ctx, cacheName.orElse(null), context, parameters);
      this.cacheOperations = operations;
      this.key = key;
   }

   /**
    * @return key.
    */
   public Optional<Object> getKey() {
      return key;
   }

   @Override
   protected InfinispanResponse execute() {
      InfinispanResponse response = InfinispanErrorResponse.asError(this, NOT_IMPLEMENTED, null);

      if (request.method() == HttpMethod.GET) {
         if (request.uri().endsWith("banner.png")) {
            response = StaticContent.INSTANCE.serveBannerFile(this);
         } else if (!getCacheName().isPresent()) {
            //we are hitting root context here
            response = StaticContent.INSTANCE.serveHtmlFile(this);
         } else if (!key.isPresent()) {
            response = cacheOperations.getCacheValues(this);
         } else {
            response = cacheOperations.getCacheValue(this);
         }
      } else if (request.method() == HttpMethod.POST || request.method() == HttpMethod.PUT) {
         response = cacheOperations.putValueToCache(this);
      } else if (request.method() == HttpMethod.HEAD) {
         response = cacheOperations.getCacheValue(this);
      } else if (request.method() == HttpMethod.DELETE) {
         if (!key.isPresent()) {
            response = cacheOperations.clearEntireCache(this);
         } else {
            response = cacheOperations.deleteCacheValue(this);
         }
      }
      return response;
   }

   /**
    * Returns <code>timeToLiveSeconds</code> header value.
    *
    * @return <code>timeToLiveSeconds</code> header value.
    */
   public Optional<Long> getTimeToLiveSeconds() {
      String timeToLiveSeconds = request.headers().get("timeToLiveSeconds");
      if (timeToLiveSeconds != null) {
         try {
            return Optional.of(Long.valueOf(timeToLiveSeconds));
         } catch (NumberFormatException ignored) {
         }
      }
      return Optional.empty();
   }

   /**
    * Returns <code>maxIdleTimeSeconds</code> header value.
    *
    * @return <code>maxIdleTimeSeconds</code> header value.
    */
   public Optional<Long> getMaxIdleTimeSeconds() {
      String maxIdleTimeSeconds = request.headers().get("maxIdleTimeSeconds");
      if (maxIdleTimeSeconds != null) {
         try {
            return Optional.of(Long.valueOf(maxIdleTimeSeconds));
         } catch (NumberFormatException ignored) {
         }
      }
      return Optional.empty();
   }

   /**
    * Returns <code>If-None-Match</code> header value.
    *
    * @return <code>If-None-Match</code> header value.
    * @see <a href="https://en.wikipedia.org/wiki/HTTP_ETag">HTTP_ETag</a>
    */
   public Optional<String> getEtagIfNoneMatch() {
      return Optional.ofNullable(request.headers().get("If-None-Match"));
   }

   /**
    * Returns <code>If-Unmodified-Since</code> header value.
    *
    * @return <code>If-Unmodified-Since</code> header value.
    * @see <a href="https://en.wikipedia.org/wiki/HTTP_ETag">HTTP_ETag</a>
    */
   public Optional<String> getEtagIfUnmodifiedSince() {
      return Optional.ofNullable(request.headers().get("If-Unmodified-Since"));
   }

   /**
    * Returns <code>If-Modified-Since</code> header value.
    *
    * @return <code>If-Modified-Since</code> header value.
    * @see <a href="https://en.wikipedia.org/wiki/HTTP_ETag">HTTP_ETag</a>
    */
   public Optional<String> getEtagIfModifiedSince() {
      return Optional.ofNullable(request.headers().get("If-Modified-Since"));
   }

   /**
    * Returns <code>If-Match</code> header value.
    *
    * @return <code>If-Match</code> header value.
    * @see <a href="https://en.wikipedia.org/wiki/HTTP_ETag">HTTP_ETag</a>
    */
   public Optional<String> getEtagIfMatch() {
      return Optional.ofNullable(request.headers().get("If-Match"));
   }

   public Optional<String> getCacheControl() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.CACHE_CONTROL));
   }

   /**
    * Returns whether client wishes to return 'Extended Headers'.
    *
    * @return <code>true</code> if client wishes to return 'Extended Headers'.
    */
   public Optional<String> getExtended() {
      List<String> extendedParameters = parameters.get("extended");
      if (extendedParameters != null && extendedParameters.size() > 0) {
         return Optional.ofNullable(extendedParameters.get(0));
      }
      return Optional.empty();
   }


}
