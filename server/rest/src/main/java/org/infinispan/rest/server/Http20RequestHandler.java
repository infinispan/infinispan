package org.infinispan.rest.server;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.server.authentication.Authenticator;
import org.infinispan.rest.server.authentication.VoidAuthenticator;
import org.infinispan.rest.server.operations.CacheOperations;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

public class Http20RequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

   protected final static Log logger = LogFactory.getLog(Http20RequestHandler.class, Log.class);

   protected final CacheOperations cacheOperations;
   protected final Authenticator authenticator;

   public Http20RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager) {
      this(configuration, embeddedCacheManager, new VoidAuthenticator());
   }

   public Http20RequestHandler(RestServerConfiguration configuration, EmbeddedCacheManager embeddedCacheManager, Authenticator authenticator) {
      this.cacheOperations = new CacheOperations(configuration, new RestCacheManager(embeddedCacheManager));
      this.authenticator = authenticator;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      InfinispanRequest infinispanRequest = new InfinispanRequest(request, ctx);
      InfinispanResponse response = InfinispanResponse.asError(NOT_IMPLEMENTED, "");

      try {
         authenticator.challenge(infinispanRequest);
         if (request.method() == HttpMethod.GET) {
            if (!infinispanRequest.getKey().isPresent()) {
               response = cacheOperations.getCacheValues(infinispanRequest);
            } else {
               response = cacheOperations.getCacheValue(infinispanRequest);
            }
         } else if (request.method() == HttpMethod.POST || request.method() == HttpMethod.PUT) {
            response = cacheOperations.putValueToCache(infinispanRequest);
         } else if (request.method() == HttpMethod.HEAD) {
            response = cacheOperations.getCacheValue(infinispanRequest);
         } else if (request.method() == HttpMethod.DELETE) {
            if (!infinispanRequest.getKey().isPresent()) {
               response = cacheOperations.clearEntireCache(infinispanRequest);
            } else {
               response = cacheOperations.deleteCacheValue(infinispanRequest);
            }
         }
      } catch (RestResponseException responseException) {
         logger.errorWhileReponding(responseException);
         response = responseException.toResponse();
      }

      sendResonse(ctx, response);
   }

   protected void sendResonse(ChannelHandlerContext ctx, InfinispanResponse response) {
      ctx.executor().execute(() -> ctx.writeAndFlush(response.toNettyHttpResponse()));
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
      // handle the case of to big requests.
      if (e.getCause() instanceof TooLongFrameException) {
         DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, REQUEST_ENTITY_TOO_LARGE);
         ctx.write(response).addListener(ChannelFutureListener.CLOSE);
      } else {
         logger.uncaughtExceptionInThePipeline(e);
         ctx.close();
      }
   }
}
