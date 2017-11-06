package org.infinispan.rest.search;

import java.io.IOException;
import java.util.Optional;

import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 9.2
 */
public class InfinispanSearchResponse extends InfinispanResponse {

   private static final ObjectMapper mapper = new ObjectMapper();

   static {
      mapper.registerSubtypes(ProjectedResult.class, QueryResult.class);
      mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
   }

   private InfinispanSearchResponse(InfinispanRequest request) {
      super(Optional.of(request));
      contentType(MediaType.APPLICATION_JSON_TYPE);
   }

   public static InfinispanSearchResponse inReplyTo(InfinispanSearchRequest infinispanSearchRequest) {
      return new InfinispanSearchResponse(infinispanSearchRequest);
   }

   public static InfinispanSearchResponse badRequest(InfinispanSearchRequest infinispanSearchRequest, String message, String cause) {
      InfinispanSearchResponse searchResponse = new InfinispanSearchResponse(infinispanSearchRequest);
      searchResponse.status(HttpResponseStatus.BAD_REQUEST);
      searchResponse.setQueryResult(new QueryErrorResult(message, cause));
      return searchResponse;
   }

   public void setQueryResult(QueryResponse queryResult) {
      try {
         byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(queryResult);
         contentAsBytes(bytes);
      } catch (IOException e) {
         throw new CacheException("Invalid query result");
      }
   }

}
