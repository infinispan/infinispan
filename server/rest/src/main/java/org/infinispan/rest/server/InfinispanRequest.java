package org.infinispan.rest.server;

import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.HttpConversionUtil;

public class InfinispanRequest {

   private final FullHttpRequest request;
   private final Optional<String> streamId;
   private final ChannelHandlerContext nettyChannelContext;
   private final String cacheName;
   private final Optional<String> key;
   private final QueryStringDecoder queryStringDecoder;


   public InfinispanRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
      this.request = request;
      queryStringDecoder = new QueryStringDecoder(request.uri());
      streamId = Optional.ofNullable(request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text()));
      this.nettyChannelContext = ctx;

      //we are parsing /rest/cacheName/key
      //not that the first one is empty!
      String[] pathParams = queryStringDecoder.path().split("/");
      switch (pathParams.length) {
         case 4:
            cacheName = pathParams[2];
            key = Optional.of(pathParams[3]);
            break;
         case 3:
            cacheName = pathParams[2];
            key = Optional.empty();
            break;
         default:
            throw new IllegalArgumentException("Wrong number of Path Parameters");
      }
   }

   public Optional<String> getStreamId() {
      return streamId;
   }

   public FullHttpRequest getRawRequest() {
      return request;
   }

   public ChannelHandlerContext getRawContext() {
      return nettyChannelContext;
   }

   public String getCacheName() {
      return cacheName;
   }

   public Optional<String> getKey() {
      return key;
   }

   public Optional<Boolean> getUseAsync() {
      String performAsync = request.headers().get("performAsync");
      if ("true".equals(performAsync)) {
         return Optional.of(Boolean.TRUE);
      }
      return Optional.empty();
   }

   public Optional<String> getAcceptContentType() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.ACCEPT));
   }

   public Optional<String> getContentType() {
      return Optional.ofNullable(request.headers().get("Content-type"));
   }

   public Optional<String> getAuthorization() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.AUTHORIZATION));
   }

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

   public Optional<Long> getTimeToLiveSeconds() {
      try {
         return Optional.of(Long.parseLong(request.headers().get("timeToLiveSeconds")));
      } catch (NumberFormatException nfe) {
         return Optional.empty();
      }
   }

   public Optional<Long> getMaxIdleTimeSeconds() {
      try {
         return Optional.of(Long.parseLong(request.headers().get("maxIdleTimeSeconds")));
      } catch (NumberFormatException nfe) {
         return Optional.empty();
      }
   }

   /**
    * @see https://en.wikipedia.org/wiki/HTTP_ETag
    */
   public Optional<String> getEtag() {
      return Optional.ofNullable(request.headers().get("If-None-Match"));
   }

   public Optional<String> getCacheControl() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.CACHE_CONTROL));
   }

   public Optional<String> getExtended() {
      List<String> extendedParameters = queryStringDecoder.parameters().get("extended");
      if(extendedParameters != null && extendedParameters.size() > 0) {
         return Optional.ofNullable(extendedParameters.get(0));
      }
      return Optional.empty();
   }
}
