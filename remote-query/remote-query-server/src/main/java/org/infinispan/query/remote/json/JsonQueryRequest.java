package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.query.remote.json.JSONConstants.OFFSET;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_MODE;
import static org.infinispan.query.remote.json.JSONConstants.QUERY_STRING;

import java.util.Map;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.query.dsl.IndexedQueryMode;

/**
 * @since 9.4
 */
public class JsonQueryRequest implements JsonSerialization {

   private static final Integer DEFAULT_OFFSET = 0;
   private static final Integer DEFAULT_MAX_RESULTS = 10;

   private final String query;
   private final Integer startOffset;
   private final Integer maxResults;
   private final IndexedQueryMode queryMode;

   public JsonQueryRequest(String query, Integer startOffset, Integer maxResults, IndexedQueryMode queryMode) {
      this.query = query;
      this.startOffset = startOffset == null ? DEFAULT_OFFSET : startOffset;
      this.maxResults = maxResults == null ? DEFAULT_MAX_RESULTS : maxResults;
      this.queryMode = queryMode;
   }

   private JsonQueryRequest(String query) {
      this(query, DEFAULT_OFFSET, DEFAULT_MAX_RESULTS, null);
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

   @Override
   public Json toJson() {
      throw new UnsupportedOperationException();
   }

   public static JsonQueryRequest fromJson(String json) {
      Map<String, Json> properties = Json.read(json).asJsonMap();
      Json queryValue = properties.get(QUERY_STRING);
      Json offsetValue = properties.get(OFFSET);
      Json maxResultsValue = properties.get(MAX_RESULTS);
      Json queryModeValue = properties.get(QUERY_MODE);

      String query = queryValue != null ? queryValue.asString() : null;
      Integer offset = offsetValue != null ? offsetValue.asInteger() : null;
      Integer maxResults = maxResultsValue != null ? maxResultsValue.asInteger() : null;
      IndexedQueryMode queryMode = queryModeValue != null ? IndexedQueryMode.valueOf(queryModeValue.asString()) : null;
      return new JsonQueryRequest(query, offset, maxResults, queryMode);
   }
}
