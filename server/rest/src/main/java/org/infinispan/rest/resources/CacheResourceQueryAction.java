package org.infinispan.rest.resources;

import static java.util.Collections.emptyMap;
import static org.infinispan.query.remote.json.JSONConstants.HIT_COUNT_ACCURACY;
import static org.infinispan.query.remote.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.query.remote.json.JSONConstants.OFFSET;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_STRING;
import static org.wildfly.security.http.HttpConstants.BAD_REQUEST;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.QueryConfiguration;
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
import org.infinispan.security.actions.SecurityActions;

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

   public CompletionStage<RestResponse> search(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);

      JsonQueryRequest query = null;
      if (request.method() == Method.GET) {
         query = getQueryFromString(request);
      }
      if (request.method() == Method.POST || request.method() == Method.PUT) {
         try {
            query = getQueryFromJSON(request);
         } catch (IOException e) {
            return CompletableFuture.completedFuture(queryError(request, "Invalid search request", e.getMessage()));
         }
      }

      if (query == null || query.getQuery() == null || query.getQuery().isEmpty()) {
         return CompletableFuture.completedFuture(queryError(request, "Invalid search request, missing 'query' parameter", null));
      }

      String cacheName = request.variables().get("cacheName");
      boolean isLocal = Boolean.parseBoolean(request.getParameter("local"));
      MediaType keyContentType = request.keyContentType();
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, keyContentType, MediaType.APPLICATION_JSON, request);

      QueryConfiguration queryConfiguration = SecurityActions.getCacheConfiguration(cache).query();
      query.setDefaultHitCountAccuracy(queryConfiguration.hitCountAccuracy());
      String queryString = query.getQuery();

      RemoteQueryManager remoteQueryManager = SecurityActions.getCacheComponentRegistry(cache).getComponent(RemoteQueryManager.class);
      JsonQueryRequest finalQuery = query;
      return CompletableFuture.supplyAsync(() -> {
         try {
            byte[] queryResultBytes = remoteQueryManager.executeQuery(queryString, emptyMap(), finalQuery.getStartOffset(),
                  finalQuery.getMaxResults(), finalQuery.getHitCountAccuracy(), cache, MediaType.APPLICATION_JSON, isLocal);
            responseBuilder.entity(queryResultBytes);
            return responseBuilder.build();
         } catch (IllegalArgumentException | ParsingException | IllegalStateException | CacheException e) {
            return queryError(request, "Error executing search", e.getMessage());
         }
      }, invocationHelper.getExecutor());
   }

   private JsonQueryRequest getQueryFromString(RestRequest request) {
      String queryString = getParameterValue(request, QUERY_STRING);
      String strOffset = getParameterValue(request, OFFSET);
      String strMaxResults = getParameterValue(request, MAX_RESULTS);
      String strHitCountAccuracy = getParameterValue(request, HIT_COUNT_ACCURACY);
      Integer offset = strOffset != null ? Integer.valueOf(strOffset) : null;
      Integer maxResults = strMaxResults != null ? Integer.valueOf(strMaxResults) : null;
      Integer hitCountAccuracy = strHitCountAccuracy != null ? Integer.valueOf(strHitCountAccuracy) : null;

      return new JsonQueryRequest(queryString, offset, maxResults, hitCountAccuracy);
   }

   private JsonQueryRequest getQueryFromJSON(RestRequest request) throws IOException {
      ContentSource contents = request.contents();
      byte[] byteContent = contents.rawContent();
      if (byteContent == null || byteContent.length == 0) throw new IOException();
      return JsonQueryRequest.fromJson(new String(byteContent, StandardCharsets.UTF_8));
   }

   private String getParameterValue(RestRequest request, String name) {
      List<String> values = request.parameters().get(name);
      return values == null ? null : values.iterator().next();
   }

   private RestResponse queryError(RestRequest request, String message, String cause) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(BAD_REQUEST);
      builder.entity(new JsonQueryErrorResult(message, cause).asBytes());
      return builder.build();
   }

}
