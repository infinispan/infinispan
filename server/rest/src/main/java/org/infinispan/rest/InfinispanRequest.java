package org.infinispan.rest;

import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.StringTokenizer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.HttpConversionUtil;

/**
 * Representation of a HTTP request tailed for Infinispan-specific requests.
 *
 * @author Sebastian Łaskawiec
 */
public class InfinispanRequest {

   private final FullHttpRequest request;
   private final Optional<String> streamId;
   private final ChannelHandlerContext nettyChannelContext;
   private final Optional<String> cacheName;
   private final String context;
   private final Optional<String> key;
   private final QueryStringDecoder queryStringDecoder;

   private InfinispanRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
      this.request = request;
      queryStringDecoder = new QueryStringDecoder(request.uri());
      streamId = Optional.ofNullable(request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text()));
      this.nettyChannelContext = ctx;

      //we are parsing /context/cacheName/key
      //not that the first one is empty!
      StringTokenizer pathTokenizer = new StringTokenizer(queryStringDecoder.path(), "/");
      context = pathTokenizer.nextToken();
      if (pathTokenizer.hasMoreTokens()) {
         cacheName = Optional.of(pathTokenizer.nextToken());
      } else {
         cacheName = Optional.empty();
      }
      if (pathTokenizer.hasMoreTokens()) {
         key = Optional.of(pathTokenizer.nextToken());
      } else {
         key = Optional.empty();
      }
   }

   /**
    * Creates new {@link InfinispanRequest} based on Netty types.
    *
    * @param request Netty request.
    * @param ctx Netty Context.
    * @return New {@link InfinispanRequest}.
    */
   public static InfinispanRequest newRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
      return new InfinispanRequest(request, ctx);
   }

   /**
    * Returns HTTP/2.0 Stream Id.
    *
    * @return HTTP/2.0 Stream Id.
    */
   public Optional<String> getStreamId() {
      return streamId;
   }

   /**
    * Returns Netty request.
    *
    * @return Netty request.
    */
   public FullHttpRequest getRawRequest() {
      return request;
   }

   /**
    * Returns Netty context.
    *
    * @return Netty context.
    */
   public ChannelHandlerContext getRawContext() {
      return nettyChannelContext;
   }

   /**
    * Returns cache name.
    *
    * @return cache name.
    */
   public Optional<String> getCacheName() {
      return cacheName;
   }

   /**
    * Returns key.
    *
    * @return key.
    */
   public Optional<String> getKey() {
      return key;
   }

   /**
    * Returns whether request should be done asynchronously.
    *
    * @return <code>true</code> if client wishes to perform request asynchronously.
    */
   public Optional<Boolean> getUseAsync() {
      String performAsync = request.headers().get("performAsync");
      if ("true".equals(performAsync)) {
         return Optional.of(Boolean.TRUE);
      }
      return Optional.empty();
   }

   /**
    * Returns <code>Accepts</code> header value.
    *
    * @return <code>Accepts</code> header value.
    */
   public Optional<String> getAcceptContentType() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.ACCEPT));
   }

   /**
    * Returns <code>Content-Type</code> header value.
    *
    * @return <code>Content-Type</code> header value.
    */
   public Optional<String> getContentType() {
      return Optional.ofNullable(request.headers().get("Content-type"));
   }

   /**
    * Returns <code>Authorization</code> header value.
    *
    * @return <code>Authorization</code> header value.
    */
   public Optional<String> getAuthorization() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.AUTHORIZATION));
   }

   /**
    * Returns request's payload.
    *
    * @return request's payload.
    */
   public Optional<byte[]> data() {
      if(request.content() != null) {
         ByteBuf content = request.content();
         if(content.hasArray()) {
            return Optional.of(content.array());
         } else {
            return Optional.of(content.copy().array());
         }
      }
      return Optional.empty();
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
         } catch (NumberFormatException nfe) {
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
         } catch (NumberFormatException nfe) {
         }
      }
      return Optional.empty();
   }

   /**
    * Returns <code>If-None-Match</code> header value.
    *
    * @return <code>If-None-Match</code> header value.
    * @see https://en.wikipedia.org/wiki/HTTP_ETag
    */
   public Optional<String> getEtagIfNoneMatch() {
      return Optional.ofNullable(request.headers().get("If-None-Match"));
   }

   /**
    * Returns <code>If-Unmodified-Since</code> header value.
    *
    * @return <code>If-Unmodified-Since</code> header value.
    * @see https://en.wikipedia.org/wiki/HTTP_ETag
    */
   public Optional<String> getEtagIfUnmodifiedSince() {
      return Optional.ofNullable(request.headers().get("If-Unmodified-Since"));
   }

   /**
    * Returns <code>If-Modified-Since</code> header value.
    *
    * @return <code>If-Modified-Since</code> header value.
    * @see https://en.wikipedia.org/wiki/HTTP_ETag
    */
   public Optional<String> getEtagIfModifiedSince() {
      return Optional.ofNullable(request.headers().get("If-Modified-Since"));
   }

   /**
    * Returns <code>If-Match</code> header value.
    *
    * @return <code>If-Match</code> header value.
    * @see https://en.wikipedia.org/wiki/HTTP_ETag
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
      List<String> extendedParameters = queryStringDecoder.parameters().get("extended");
      if(extendedParameters != null && extendedParameters.size() > 0) {
         return Optional.ofNullable(extendedParameters.get(0));
      }
      return Optional.empty();
   }

   /**
    * Returns Netty context.
    *
    * @return Netty context.
    */
   public String getContext() {
      return context;
   }
}
