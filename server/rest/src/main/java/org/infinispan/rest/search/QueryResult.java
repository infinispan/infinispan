package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.HITS;
import static org.infinispan.rest.JSONConstants.TOTAL_RESULTS;

import java.util.List;

import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({TOTAL_RESULTS, HITS})
public class QueryResult extends BaseQueryResult {

   private final List<Hit> hits;

   public QueryResult(List<Hit> hits, int total) {
      super(total);
      this.hits = hits;
   }

   public List<Hit> getHits() {
      return hits;
   }

}
