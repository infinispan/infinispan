package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HITS;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @since 9.4
 */
@JsonPropertyOrder({TOTAL_RESULTS, HITS})
public class ProjectedJsonResult extends BaseJsonQueryResult {

   private final List<JsonProjection> hits;

   public ProjectedJsonResult(int totalResults, String[] projections, List<Object> values) {
      super(totalResults);
      hits = new ArrayList<>(projections.length);
      for (Object v : values) {
         Object[] result = (Object[]) v;
         Map<String, Object> p = new HashMap<>();
         for (int i = 0; i < projections.length; i++) {
            p.put(projections[i], result[i]);
         }
         hits.add(new JsonProjection(p));
      }
   }

   public List<JsonProjection> getHits() {
      return hits;
   }
}
