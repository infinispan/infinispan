package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.util.concurrent.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class BaseHttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

   protected final RestAccessLoggingHandler restAccessLoggingHandler = new RestAccessLoggingHandler();

   protected void handleError(ChannelHandlerContext ctx, FullHttpRequest request, Throwable throwable) {
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
         errorResponse = new NettyRestResponse.Builder().status(INTERNAL_SERVER_ERROR).entity(cause.getMessage()).build();
      }
      sendResponse(ctx, request, errorResponse.getResponse());
   }

   protected void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
      ctx.executor().execute(() -> {
         restAccessLoggingHandler.log(ctx, request, response);
         request.release();
         ctx.writeAndFlush(response);
      });
   }

   protected abstract Log getLogger();
}
