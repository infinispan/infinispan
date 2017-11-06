package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.HITS;
import static org.infinispan.rest.JSONConstants.TOTAL_RESULTS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({TOTAL_RESULTS, HITS})
public class ProjectedResult extends BaseQueryResult {

   private final List<Projection> hits;

   public ProjectedResult(int totalResults, String[] projections, List<Object> values) {
      super(totalResults);
      hits = new ArrayList<>(projections.length);
      for (Object result1 : values) {
         Object[] result = (Object[]) result1;
         Map<String, Object> p = new HashMap<>();
         for (int j = 0; j < projections.length; j++) {
            p.put(projections[j], result[j]);
         }
         hits.add(new Projection(p));
      }
   }

   public List<Projection> getHits() {
      return hits;
   }

}
