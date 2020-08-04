package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.util.Collections.emptyMap;
import static org.infinispan.query.remote.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.query.remote.json.JSONConstants.OFFSET;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_MODE;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_STRING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.infinispan.query.remote.json.JsonQueryErrorResult;
import org.infinispan.query.remote.json.JsonQueryRequest;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

/**
 * Helper for handling the 'search' action of the {@link BaseCacheResource}.
 *
 * @since 10.0
 */
class CacheResourceQueryAction {

   private final InvocationHelper invocationHelper;

   CacheResourceQueryAction(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   public CompletionStage<RestResponse> search(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      JsonQueryRequest query = null;
      if (restRequest.method() == Method.GET) {
         query = getQueryFromString(restRequest);
      }
      if (restRequest.method() == Method.POST || restRequest.method() == Method.PUT) {
         try {
            query = getQueryFromJSON(restRequest);
         } catch (IOException e) {
            return CompletableFuture.completedFuture(queryError("Invalid search request", e.getMessage()));
         }
      }

      if (query == null || query.getQuery() == null || query.getQuery().isEmpty()) {
         return CompletableFuture.completedFuture(queryError("Invalid search request, missing 'query' parameter", null));
      }

      String cacheName = restRequest.variables().get("cacheName");

      MediaType keyContentType = restRequest.keyContentType();
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, keyContentType, MediaType.APPLICATION_JSON, restRequest);
      String queryString = query.getQuery();

      RemoteQueryManager remoteQueryManager = cache.getComponentRegistry().getComponent(RemoteQueryManager.class);
      JsonQueryRequest finalQuery = query;
      return CompletableFuture.supplyAsync(() -> {
         try {
            byte[] queryResultBytes = remoteQueryManager.executeQuery(queryString, emptyMap(), finalQuery.getStartOffset(),
                  finalQuery.getMaxResults(), cache, MediaType.APPLICATION_JSON);
            responseBuilder.entity(queryResultBytes);
            return responseBuilder.build();
         } catch (IllegalArgumentException | ParsingException | IllegalStateException | CacheException e) {
            return queryError("Error executing search", e.getMessage());
         }
      }, invocationHelper.getExecutor());
   }

   private JsonQueryRequest getQueryFromString(RestRequest restRequest) {
      String queryString = getParameterValue(restRequest, QUERY_STRING);
      String strOffset = getParameterValue(restRequest, OFFSET);
      String queryMode = getParameterValue(restRequest, QUERY_MODE);
      String strMaxResults = getParameterValue(restRequest, MAX_RESULTS);
      Integer offset = strOffset != null ? Integer.valueOf(strOffset) : null;
      Integer maxResults = strMaxResults != null ? Integer.valueOf(strMaxResults) : null;
      return new JsonQueryRequest(queryString, offset, maxResults);
   }

   private JsonQueryRequest getQueryFromJSON(RestRequest restRequest) throws IOException {
      ContentSource contents = restRequest.contents();
      byte[] byteContent = contents.rawContent();
      if (byteContent == null || byteContent.length == 0) throw new IOException();
      return JsonQueryRequest.fromJson(new String(byteContent, StandardCharsets.UTF_8));
   }

   private String getParameterValue(RestRequest restRequest, String name) {
      List<String> values = restRequest.parameters().get(name);
      return values == null ? null : values.iterator().next();
   }

   private RestResponse queryError(String message, String cause) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(BAD_REQUEST);
      builder.entity(new JsonQueryErrorResult(message, cause).asBytes());
      return builder.build();
   }

}
