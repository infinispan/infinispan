package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @since 9.4
 */

class BaseJsonQueryResult extends JsonQueryResponse {

   private int totalResults;

   @JsonCreator
   BaseJsonQueryResult(@JsonProperty(TOTAL_RESULTS) int totalResults) {
      this.totalResults = totalResults;
   }

   @JsonProperty(TOTAL_RESULTS)
   public int getTotalResults() {
      return totalResults;
   }

}
