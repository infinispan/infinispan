package org.infinispan.rest.search;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static org.infinispan.rest.JSONConstants.MAX_RESULTS;
import static org.infinispan.rest.JSONConstants.OFFSET;
import static org.infinispan.rest.JSONConstants.QUERY_MODE;
import static org.infinispan.rest.JSONConstants.QUERY_STRING;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.rest.InfinispanErrorResponse;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.operations.SearchOperations;
import org.infinispan.rest.operations.exceptions.NoCacheFoundException;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * @since 9.2
 */
public class InfinispanSearchRequest extends InfinispanRequest {

   private final ObjectMapper mapper = new ObjectMapper();

   private final SearchOperations searchOperations;

   public InfinispanSearchRequest(SearchOperations searchOperations, FullHttpRequest request, ChannelHandlerContext ctx, String cacheName, String context, Map<String, List<String>> parameters) {
      super(request, ctx, cacheName, context, parameters);
      this.searchOperations = searchOperations;
   }

   @Override
   protected InfinispanResponse execute() {
      Optional<String> cacheName = getCacheName();
      if (!cacheName.isPresent()) {
         throw new NoCacheFoundException("Cache name must be provided");
      }
      try {
         QueryRequest queryRequest;
         switch (request.method().name()) {
            case "GET":
            case "HEAD":
            case "OPTIONS":
               queryRequest = getQueryFromString();
               break;
            case "POST":
            case "PUT":
               queryRequest = getQueryFromJSON();
               break;
            default:
               return InfinispanErrorResponse.asError(this, NOT_IMPLEMENTED, null);
         }
         String queryString = queryRequest.getQuery();
         if (queryString == null || queryString.isEmpty()) {
            return InfinispanSearchResponse.badRequest(this, "Invalid search request, missing 'query' parameter", null);
         }
         return searchOperations.search(cacheName.get(), queryRequest, this);
      } catch (IOException e) {
         return InfinispanSearchResponse.badRequest(this, "Invalid search request", e.getMessage());
      }
   }


   private QueryRequest getQueryFromString() {
      String queryString = getParameterValue(QUERY_STRING);
      String strOffset = getParameterValue(OFFSET);
      String queryMode = getParameterValue(QUERY_MODE);
      String strMaxResults = getParameterValue(MAX_RESULTS);
      Integer offset = strOffset != null ? Integer.valueOf(strOffset) : null;
      Integer maxResults = strMaxResults != null ? Integer.valueOf(strMaxResults) : null;
      IndexedQueryMode qm = queryMode == null ? IndexedQueryMode.FETCH : IndexedQueryMode.valueOf(queryMode);
      return new QueryRequest(queryString, offset, maxResults, qm);
   }

   private QueryRequest getQueryFromJSON() throws IOException {
      byte[] data = data().orElseThrow(NoDataFoundException::new);
      return mapper.readValue(data, QueryRequest.class);
   }
}
