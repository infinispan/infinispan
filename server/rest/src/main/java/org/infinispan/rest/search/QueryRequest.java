package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.MAX_RESULTS;
import static org.infinispan.rest.JSONConstants.OFFSET;
import static org.infinispan.rest.JSONConstants.QUERY_STRING;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public class QueryRequest {

   private static final Integer DEFAULT_OFFSET = 0;
   private static final Integer DEFAULT_MAX_RESULTS = 10;

   @JsonProperty(QUERY_STRING)
   private final String query;

   @JsonProperty(OFFSET)
   private final Integer startOffset;

   @JsonProperty(MAX_RESULTS)
   private final Integer maxResults;

   QueryRequest(String query, Integer startOffset, Integer maxResults) {
      this.query = query;
      this.startOffset = startOffset == null ? DEFAULT_OFFSET : startOffset;
      this.maxResults = maxResults == null ? DEFAULT_MAX_RESULTS : maxResults;
   }

   private QueryRequest(String query) {
      this.query = query;
      this.startOffset = DEFAULT_OFFSET;
      this.maxResults = DEFAULT_MAX_RESULTS;
   }

   private QueryRequest() {
      this("");
   }

   public String getQuery() {
      return query;
   }

   public Integer getStartOffset() {
      return startOffset;
   }

   public Integer getMaxResults() {
      return maxResults;
   }
}
