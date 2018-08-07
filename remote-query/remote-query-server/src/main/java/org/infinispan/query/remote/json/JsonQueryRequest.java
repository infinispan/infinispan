package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.query.remote.json.JSONConstants.OFFSET;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_MODE;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_STRING;

import org.infinispan.query.dsl.IndexedQueryMode;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @since 9.4
 */
@SuppressWarnings("unused")
public class JsonQueryRequest {

   private static final Integer DEFAULT_OFFSET = 0;
   private static final Integer DEFAULT_MAX_RESULTS = 10;

   @JsonProperty(QUERY_STRING)
   private final String query;

   @JsonProperty(OFFSET)
   private final Integer startOffset;

   @JsonProperty(MAX_RESULTS)
   private final Integer maxResults;

   @JsonProperty(QUERY_MODE)
   private IndexedQueryMode queryMode;

   public JsonQueryRequest(String query, Integer startOffset, Integer maxResults, IndexedQueryMode queryMode) {
      this.query = query;
      this.startOffset = startOffset == null ? DEFAULT_OFFSET : startOffset;
      this.maxResults = maxResults == null ? DEFAULT_MAX_RESULTS : maxResults;
      this.queryMode = queryMode;
   }

   private JsonQueryRequest(String query) {
      this(query, DEFAULT_OFFSET, DEFAULT_MAX_RESULTS, IndexedQueryMode.FETCH);
   }

   private JsonQueryRequest() {
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

   public IndexedQueryMode getQueryMode() {
      return queryMode;
   }
}
