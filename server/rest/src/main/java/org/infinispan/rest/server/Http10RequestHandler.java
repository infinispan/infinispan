package org.infinispan.rest.server;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.server.authentication.Authenticator;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpUtil;

public class Http10RequestHandler extends Http20RequestHandler {

   public Http10RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager) {
      super(configuration, embeddedCacheManager);
   }

   public Http10RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager, Authenticator authenticator) {
      super(configuration, embeddedCacheManager, authenticator);
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      if (HttpUtil.is100ContinueExpected(request)) {
         ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
      }
      super.channelRead0(ctx, request);
   }

   @Override
   protected void sendResonse(ChannelHandlerContext ctx, InfinispanResponse response) {
      ctx.executor().execute(() -> {
         if (response.isKeepAlive()) {
            ctx.writeAndFlush(response.toNettyHttpResponse());
         } else {
            ctx.writeAndFlush(response.toNettyHttpResponse()).addListener(ChannelFutureListener.CLOSE);
         }
      });
   }
}
