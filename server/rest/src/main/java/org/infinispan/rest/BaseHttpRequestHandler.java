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
      } else if (cause instanceof CacheConfigurationException || cause instanceof IllegalArgumentException) {
         errorResponse = new NettyRestResponse.Builder().status(BAD_REQUEST).entity(cause.getMessage()).build();
      } else {
         Throwable rootCause = Util.getRootCause(throwable);
         errorResponse = new NettyRestResponse.Builder().status(INTERNAL_SERVER_ERROR).entity(rootCause.getMessage()).build();
      }
      sendResponse(ctx, request, errorResponse);
   }

   void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
      ctx.executor().execute(() -> ResponseWriter.forContent(response.getEntity()).writeResponse(ctx, request, response));
   }

   protected abstract Log getLogger();
}
