package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.authentication.Authenticator;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;

/**
 * Netty REST handler for HTTP/1.1
 *
 * @author Sebastian Łaskawiec
 */
public class Http11RequestHandler extends Http20RequestHandler {

   /**
    * Creates new {@link Http11RequestHandler}.
    *
    * @param configuration Rest Server configuration
    * @param embeddedCacheManager Embedded Cache Manager for storing data.
    */
   public Http11RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager) {
      super(configuration, embeddedCacheManager);
   }

   /**
    * Creates new {@link Http11RequestHandler}.
    *
    * @param configuration Rest Server configuration
    * @param embeddedCacheManager Embedded Cache Manager for storing data.
    * @param authenticator Authenticator.
    */
   public Http11RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager, Authenticator authenticator) {
      super(configuration, embeddedCacheManager, authenticator);
   }

   @Override
   public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      if (HttpUtil.is100ContinueExpected(request)) {
         ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
      }
      super.channelRead0(ctx, request);
   }

   @Override
   protected void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
      ctx.executor().execute(() -> {
         restAccessLoggingHandler.log(ctx, request, response);
         if (HttpUtil.isKeepAlive(response)) {
            ctx.writeAndFlush(response);
         } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
         }
      });
   }
}
