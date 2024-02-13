package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HITS;
import static org.infinispan.query.remote.json.JSONConstants.HIT_COUNT;
import static org.infinispan.query.remote.json.JSONConstants.HIT_COUNT_EXACT;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @since 9.4
 */
public class ProjectedJsonResult extends JsonQueryResponse {

   private final List<JsonProjection> hits;

   public ProjectedJsonResult(int hitCount, boolean hitCountExact, List<JsonProjection> hits) {
      super(hitCount, hitCountExact);
      this.hits = hits;
   }

   public List<JsonProjection> getHits() {
      return hits;
   }

   @Override
   public Json toJson() {
      Json object = Json.object();
      object.set(HIT_COUNT, hitCount());
      object.set(HIT_COUNT_EXACT, hitCountExact());
      Json array = Json.array();
      hits.forEach(h -> array.add(Json.factory().raw(h.toJson().toString())));
      return object.set(HITS, array);
   }
}
