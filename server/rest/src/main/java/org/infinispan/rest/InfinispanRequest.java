package org.infinispan.rest;

import java.util.Optional;

import org.infinispan.rest.operations.CacheOperations;

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
public abstract class InfinispanRequest {

   protected final FullHttpRequest request;
   private final Optional<String> streamId;
   private final ChannelHandlerContext nettyChannelContext;
   private final String cacheName;
   private final String context;
   final QueryStringDecoder queryStringDecoder;

   protected InfinispanRequest(FullHttpRequest request, ChannelHandlerContext ctx, String cacheName, String context) {
      this.request = request;
      this.queryStringDecoder = new QueryStringDecoder(request.uri());
      this.streamId = Optional.ofNullable(request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text()));
      this.nettyChannelContext = ctx;
      this.cacheName = cacheName;
      this.context = context;
   }

   protected abstract InfinispanResponse execute(CacheOperations cacheOperations);

   /**
    * @return cache name.
    */
   public Optional<String> getCacheName() {
      return Optional.ofNullable(cacheName);
   }

   /***
    * @return HTTP/2.0 Stream Id.
    */
   public Optional<String> getStreamId() {
      return streamId;
   }

   /***
    * @return Netty request.
    */
   public FullHttpRequest getRawRequest() {
      return request;
   }

   /**
    * @return Netty context.
    */
   public ChannelHandlerContext getRawContext() {
      return nettyChannelContext;
   }

   /***
    * @return <code>true</code> if client wishes to perform request asynchronously.
    */
   public Optional<Boolean> getUseAsync() {
      String performAsync = request.headers().get("performAsync");
      if ("true".equals(performAsync)) {
         return Optional.of(Boolean.TRUE);
      }
      return Optional.empty();
   }

   /***
    * @return <code>Accepts</code> header value.
    */
   public Optional<String> getAcceptContentType() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.ACCEPT));
   }

   /***
    * @return <code>Content-Type</code> header value.
    */
   public Optional<String> getContentType() {
      return Optional.ofNullable(request.headers().get("Content-type"));
   }

   /***
    * @return <code>Authorization</code> header value.
    */
   public Optional<String> getAuthorization() {
      return Optional.ofNullable(request.headers().get(HttpHeaderNames.AUTHORIZATION));
   }

   /***
    * @return Netty context.
    */
   public String getContext() {
      return context;
   }

   /***
    * @return request's payload.
    */
   public Optional<byte[]> data() {
      if (request.content() != null) {
         ByteBuf content = request.content();
         if (content.hasArray()) {
            return Optional.of(content.array());
         } else {
            byte[] bufferCopy = new byte[content.readableBytes()];
            content.readBytes(bufferCopy);
            return Optional.of(bufferCopy);
         }
      }
      return Optional.empty();
   }
}
