package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HITS;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @since 9.4
 */
public class ProjectedJsonResult extends JsonQueryResponse {

   private final List<JsonProjection> hits;

   public ProjectedJsonResult(long totalResults, List<JsonProjection> hits) {
      super(totalResults);
      this.hits = hits;
   }

   public List<JsonProjection> getHits() {
      return hits;
   }

   @Override
   public Json toJson() {
      Json object = Json.object();
      object.set(TOTAL_RESULTS, getTotalResults());
      Json array = Json.array();
      hits.forEach(h -> array.add(Json.factory().raw(h.toJson().toString())));
      return object.set(HITS, array);
   }
}
