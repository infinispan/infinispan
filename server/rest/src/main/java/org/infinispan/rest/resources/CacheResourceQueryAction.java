package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.util.Collections.emptyMap;
import static org.infinispan.query.remote.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.query.remote.json.JSONConstants.OFFSET;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_MODE;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_STRING;

import java.io.IOException;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.infinispan.query.remote.json.JsonQueryErrorResult;
import org.infinispan.query.remote.json.JsonQueryReader;
import org.infinispan.query.remote.json.JsonQueryRequest;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;

/**
 * Helper for handling the 'search' action of the {@link CacheResource}.
 *
 * @since 10.0
 */
class CacheResourceQueryAction {

   private final RestCacheManager<Object> restCacheManager;

   CacheResourceQueryAction(RestCacheManager<Object> restCacheManager) {
      this.restCacheManager = restCacheManager;
   }

   public NettyRestResponse search(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      try {
         JsonQueryRequest query = null;
         if (restRequest.method() == Method.GET) {
            query = getQueryFromString(restRequest);
         }
         if (restRequest.method() == Method.POST || restRequest.method() == Method.PUT) {
            query = getQueryFromJSON(restRequest);
         }


         if (query == null || query.getQuery() == null || query.getQuery().isEmpty()) {
            return queryError("Invalid search request, missing 'query' parameter", null);
         }

         String cacheName = restRequest.variables().get("cacheName");

         MediaType keyContentType = restRequest.keyContentType();
         AdvancedCache<Object, Object> cache = restCacheManager.getCache(cacheName, keyContentType, MediaType.APPLICATION_JSON);
         String queryString = query.getQuery();

         RemoteQueryManager remoteQueryManager = cache.getComponentRegistry().getComponent(RemoteQueryManager.class);
         byte[] queryResultBytes = remoteQueryManager.executeQuery(queryString, emptyMap(), query.getStartOffset(),
               query.getMaxResults(), query.getQueryMode(), cache, MediaType.APPLICATION_JSON);
         responseBuilder.entity(queryResultBytes);
         return responseBuilder.build();
      } catch (IllegalArgumentException | ParsingException | IllegalStateException | IOException e) {
         return queryError("Invalid search request", e.getMessage());
      }

   }

   private JsonQueryRequest getQueryFromString(RestRequest restRequest) {
      String queryString = getParameterValue(restRequest, QUERY_STRING);
      String strOffset = getParameterValue(restRequest, OFFSET);
      String queryMode = getParameterValue(restRequest, QUERY_MODE);
      String strMaxResults = getParameterValue(restRequest, MAX_RESULTS);
      Integer offset = strOffset != null ? Integer.valueOf(strOffset) : null;
      Integer maxResults = strMaxResults != null ? Integer.valueOf(strMaxResults) : null;
      IndexedQueryMode qm = queryMode == null ? IndexedQueryMode.FETCH : IndexedQueryMode.valueOf(queryMode);
      return new JsonQueryRequest(queryString, offset, maxResults, qm);
   }

   private JsonQueryRequest getQueryFromJSON(RestRequest restRequest) throws IOException {
      ContentSource contents = restRequest.contents();
      byte[] byteContent = contents.rawContent();
      if (byteContent == null || byteContent.length == 0) throw new IOException();
      return JsonQueryReader.getQueryFromJSON(byteContent);
   }

   private String getParameterValue(RestRequest restRequest, String name) {
      List<String> values = restRequest.parameters().get(name);
      return values == null ? null : values.iterator().next();
   }

   private NettyRestResponse queryError(String message, String cause) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(BAD_REQUEST);
      builder.entity(new JsonQueryErrorResult(message, cause).asBytes());
      return builder.build();
   }

}
