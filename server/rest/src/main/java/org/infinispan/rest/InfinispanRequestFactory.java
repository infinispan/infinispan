package org.infinispan.rest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.rest.operations.exceptions.MalformedRequest;
import org.infinispan.rest.search.InfinispanSearchRequest;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * @since 9.2
 */
class InfinispanRequestFactory {

   private static final String SEARCH_ACTION = "search";
   private static final String ACTION_PARAMETER = "action";

   private InfinispanRequestFactory() {
   }

   /**
    * Creates the appropriate {@link InfinispanRequest} instance based on the raw incoming request.
    */
   static InfinispanRequest createRequest(RestServer restServer, FullHttpRequest request, ChannelHandlerContext ctx) {
      QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
      Map<String, List<String>> parameters = queryStringDecoder.parameters();

      // Obtain each of the path components excluding the context, e.g. from '/rest/cache/k' obtain ['cache', 'k']
      String[] components = queryStringDecoder.path().substring(1).split("/");

      String context;
      Optional<Object> key = Optional.empty();
      Optional<String> cacheName = Optional.empty();

      List<String> actionParameter = parameters.get(ACTION_PARAMETER);

      if (components.length > 3 || components.length == 0) {
         throw new MalformedRequest("Invalid request path");
      }
      Iterator<String> pathElements = Arrays.stream(components).iterator();
      context = pathElements.next();
      if (pathElements.hasNext()) {
         cacheName = Optional.of(pathElements.next());
      }
      if (pathElements.hasNext()) {
         key = Optional.of(pathElements.next());
      }

      if (actionParameter == null || actionParameter.isEmpty()) {
         // Cache API request
         return new InfinispanCacheAPIRequest(restServer.getCacheOperations(), request, ctx, cacheName, key, context, parameters);
      }

      if (actionParameter.size() > 1) {
         throw new MalformedRequest("The 'action' parameter must contain only one value");
      }

      String action = actionParameter.iterator().next();

      switch (action) {
         case SEARCH_ACTION:
            if (!cacheName.isPresent()) {
               throw new MalformedRequest("Missing cacheName");
            }
            return new InfinispanSearchRequest(restServer.getSearchOperations(), request, ctx, cacheName.get(), context, parameters);
         default:
            throw new MalformedRequest("Invalid action");
      }

   }
}
