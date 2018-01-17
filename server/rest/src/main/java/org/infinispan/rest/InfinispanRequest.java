package org.infinispan.rest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.dataconversion.MediaType;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.HttpConversionUtil;

/**
 * Representation of a HTTP request tailed for Infinispan-specific requests.
 *
 * @author Sebastian Łaskawiec
 */
public abstract class InfinispanRequest {

   private static final String KEY_CONTENT_TYPE = "Key-Content-Type";
   private static final MediaType DEFAULT_KEY_CONTENT_TYPE = MediaType.parse("application/x-java-object;type=java.lang.String");
   protected final FullHttpRequest request;
   private final Optional<String> streamId;
   private final ChannelHandlerContext nettyChannelContext;
   private final String cacheName;
   private final String context;
   protected Map<String, List<String>> parameters;

   protected InfinispanRequest(FullHttpRequest request, ChannelHandlerContext ctx, String cacheName, String context, Map<String, List<String>> parameters) {
      this.request = request;
      this.streamId = Optional.ofNullable(request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text()));
      this.nettyChannelContext = ctx;
      this.cacheName = cacheName;
      this.context = context;
      this.parameters = parameters;
   }

   protected abstract InfinispanResponse execute();

   /**
    * @return cache name.
    */
   public Optional<String> getCacheName() {
      return Optional.ofNullable(cacheName);
   }

   /***
    * @return HTTP/2.0 Stream Id.
    */
   Optional<String> getStreamId() {
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

   public MediaType getKeyContentType() {
      return Optional.ofNullable(request.headers().get(KEY_CONTENT_TYPE)).map(MediaType::parse)
            .orElse(DEFAULT_KEY_CONTENT_TYPE);
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

   protected String getParameterValue(String name) {
      List<String> values = parameters.get(name);
      if (values == null) return null;
      return values.iterator().next();
   }
}
