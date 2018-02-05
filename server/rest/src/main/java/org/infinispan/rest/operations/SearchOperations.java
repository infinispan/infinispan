package org.infinispan.rest.operations;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.infinispan.query.remote.impl.RemoteQueryResult;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.search.Hit;
import org.infinispan.rest.search.InfinispanSearchRequest;
import org.infinispan.rest.search.InfinispanSearchResponse;
import org.infinispan.rest.search.ProjectedResult;
import org.infinispan.rest.search.QueryRequest;
import org.infinispan.rest.search.QueryResult;

/**
 * Handle search related operations via Rest.
 *
 * @since 9.2
 */
public class SearchOperations extends AbstractOperations {

   public SearchOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      super(configuration, cacheManager);
   }

   public InfinispanSearchResponse search(String cacheName, QueryRequest query, InfinispanSearchRequest request) {
      InfinispanSearchResponse searchResponse = InfinispanSearchResponse.inReplyTo(request);
      MediaType keyContentType = request.getKeyContentType();
      AdvancedCache<Object, Object> cache = restCacheManager.getCache(cacheName, keyContentType, MediaType.APPLICATION_JSON, request.getSubject());
      String queryString = query.getQuery();
      try {
         RemoteQueryManager remoteQueryManager = cache.getComponentRegistry().getComponent(RemoteQueryManager.class);
         RemoteQueryResult remoteQueryResult = remoteQueryManager.executeQuery(queryString, query.getStartOffset(), query.getMaxResults(), query.getQueryMode());
         int totalResults = remoteQueryResult.getTotalResults();
         List<Object> results = remoteQueryResult.getResults();
         String[] projections = remoteQueryResult.getProjections();
         if (projections == null) {
            List<Hit> hits = results.stream().map(Hit::new).collect(Collectors.toList());
            QueryResult queryResult = new QueryResult(hits, totalResults);
            searchResponse.setQueryResult(queryResult);
            return searchResponse;
         } else {
            ProjectedResult projectedResult = new ProjectedResult(totalResults, projections, results);
            searchResponse.setQueryResult(projectedResult);
            return searchResponse;
         }
      } catch (IllegalArgumentException | ParsingException | IllegalStateException e) {
         return InfinispanSearchResponse.badRequest(request, "Error executing query", e.getMessage());
      }

   }

}
