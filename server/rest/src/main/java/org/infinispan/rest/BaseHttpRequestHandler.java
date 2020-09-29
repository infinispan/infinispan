package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.util.concurrent.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class BaseHttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

   BaseHttpRequestHandler() {
      super(false);
   }

   final RestAccessLoggingHandler restAccessLoggingHandler = new RestAccessLoggingHandler();

   void handleError(ChannelHandlerContext ctx, FullHttpRequest request, Throwable throwable) {
      Throwable cause = CompletableFutures.extractException(throwable);
      NettyRestResponse errorResponse;
      if (cause instanceof RestResponseException) {
         RestResponseException responseException = (RestResponseException) throwable;
         getLogger().errorWhileResponding(responseException);
         errorResponse = new NettyRestResponse.Builder().status(responseException.getStatus()).entity(responseException.getText()).build();
      } else if (cause instanceof SecurityException) {
         errorResponse = new NettyRestResponse.Builder().status(FORBIDDEN).entity(cause.getMessage()).build();
      } else if (cause instanceof CacheConfigurationException) {
         errorResponse = new NettyRestResponse.Builder().status(BAD_REQUEST).entity(cause.getMessage()).build();
      } else {
         Throwable rootCause = Util.getRootCause(throwable);
         errorResponse = new NettyRestResponse.Builder().status(INTERNAL_SERVER_ERROR).entity(rootCause.getMessage()).build();
      }
      sendResponse(ctx, request, errorResponse);
   }

   void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
      ctx.executor().execute(() -> {
         addCorrelatedHeaders(request, response.getResponse());
         ResponseWriter.forContent(response.getEntity()).writeResponse(ctx, request, response, checkKeepAlive());
      });
   }

   private void addCorrelatedHeaders(FullHttpRequest request, HttpResponse response) {
      String streamId = request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
      if (streamId != null) {
         response.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
      }
      boolean isKeepAlive = HttpUtil.isKeepAlive(request);
      HttpVersion httpVersion = response.protocolVersion();
      if ((httpVersion == HttpVersion.HTTP_1_1 || httpVersion == HttpVersion.HTTP_1_0) && isKeepAlive) {
         response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
   }

   protected boolean checkKeepAlive() {
      return false;
   }

   protected abstract Log getLogger();
}
