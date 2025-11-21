package org.infinispan.server.core.query.json;

import static org.infinispan.server.core.query.json.JSONConstants.HIT_COUNT_ACCURACY;
import static org.infinispan.server.core.query.json.JSONConstants.MAX_RESULTS;
import static org.infinispan.server.core.query.json.JSONConstants.OFFSET;
import static org.infinispan.server.core.query.json.JSONConstants.QUERY_STRING;

import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @since 9.4
 */
public class JsonQueryRequest implements JsonSerialization {

   private static final int DEFAULT_OFFSET = 0;
   private static final int DEFAULT_MAX_RESULTS = 10;

   private final String query;
   private final Integer startOffset;
   private final Integer maxResults;

   private Integer hitCountAccuracy;

   public JsonQueryRequest(String query, Integer startOffset, Integer maxResults, Integer hitCountAccuracy) {
      this.query = query;
      this.startOffset = startOffset == null ? DEFAULT_OFFSET : startOffset;
      this.maxResults = maxResults == null ? DEFAULT_MAX_RESULTS : maxResults;
      this.hitCountAccuracy = hitCountAccuracy;
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

   public Integer getHitCountAccuracy() {
      return hitCountAccuracy;
   }

   public void setDefaultHitCountAccuracy(int defaultHitCountAccuracy) {
      if (hitCountAccuracy == null) {
         hitCountAccuracy = defaultHitCountAccuracy;
      }
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
      Json hitCountAccuracyValue = properties.get(HIT_COUNT_ACCURACY);

      String query = queryValue != null ? queryValue.asString() : null;
      Integer offset = offsetValue != null ? offsetValue.asInteger() : null;
      Integer maxResults = maxResultsValue != null ? maxResultsValue.asInteger() : null;
      Integer hitCountAccuracy = hitCountAccuracyValue != null ? hitCountAccuracyValue.asInteger() : null;
      return new JsonQueryRequest(query, offset, maxResults, hitCountAccuracy);
   }
}
