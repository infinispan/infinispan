package org.infinispan.rest.operations;

import static java.util.Collections.emptyMap;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.infinispan.query.remote.json.JsonQueryRequest;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.search.InfinispanSearchRequest;
import org.infinispan.rest.search.InfinispanSearchResponse;

/**
 * Handle search related operations via Rest.
 *
 * @since 9.2
 */
public class SearchOperations extends AbstractOperations {

   public SearchOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      super(configuration, cacheManager);
   }

   public InfinispanSearchResponse search(String cacheName, JsonQueryRequest query, InfinispanSearchRequest request) {
      InfinispanSearchResponse searchResponse = InfinispanSearchResponse.inReplyTo(request);
      MediaType keyContentType = request.getKeyContentType();
      AdvancedCache<Object, Object> cache = restCacheManager.getCache(cacheName, keyContentType, MediaType.APPLICATION_JSON);
      String queryString = query.getQuery();
      try {
         RemoteQueryManager remoteQueryManager = cache.getComponentRegistry().getComponent(RemoteQueryManager.class);
         byte[] queryResultBytes = remoteQueryManager.executeQuery(queryString, emptyMap(), query.getStartOffset(),
               query.getMaxResults(), query.getQueryMode(), cache, MediaType.APPLICATION_JSON);
         searchResponse.contentAsBytes(queryResultBytes);
         return searchResponse;
      } catch (IllegalArgumentException | ParsingException | IllegalStateException e) {
         return InfinispanSearchResponse.badRequest(request, "Error executing query", e.getMessage());
      }

   }

}
