package org.infinispan.rest;

import java.util.Optional;
import java.util.StringTokenizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * @since 9.2
 */
class InfinispanRequestCreator {

   /**
    * Creates the appropriate {@link InfinispanRequest} instance based on the raw incoming request.
    */
   static InfinispanRequest createRequest(FullHttpRequest request, ChannelHandlerContext ctx) {
      QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());

      StringTokenizer pathTokenizer = new StringTokenizer(queryStringDecoder.path(), "/");
      String context = pathTokenizer.nextToken();
      Optional<String> cacheName, key;
      if (pathTokenizer.hasMoreTokens()) {
         String nextToken = pathTokenizer.nextToken();
         cacheName = Optional.of(nextToken);
      } else {
         cacheName = Optional.empty();
      }
      if (pathTokenizer.hasMoreTokens()) {
         String next = pathTokenizer.nextToken();
         key = Optional.of(next);
      } else {
         key = Optional.empty();
      }
      return new InfinispanCacheAPIRequest(request, ctx, cacheName, key, context);
   }
}
