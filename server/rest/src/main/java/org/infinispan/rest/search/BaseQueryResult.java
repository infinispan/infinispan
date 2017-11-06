package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.TOTAL_RESULTS;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @since 9.2
 */

@SuppressWarnings("unused")
class BaseQueryResult implements QueryResponse {

   private int totalResults;

   @JsonCreator
   BaseQueryResult(@JsonProperty(TOTAL_RESULTS) int totalResults) {
      this.totalResults = totalResults;
   }

   @JsonProperty(TOTAL_RESULTS)
   public int getTotalResults() {
      return totalResults;
   }

}
