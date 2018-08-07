package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HITS;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @since 9.4
 */
@JsonPropertyOrder({TOTAL_RESULTS, HITS})
public class JsonQueryResult extends BaseJsonQueryResult {

   private final List<Hit> hits;

   public JsonQueryResult(List<Hit> hits, int total) {
      super(total);
      this.hits = hits;
   }

   public List<Hit> getHits() {
      return hits;
   }

}
