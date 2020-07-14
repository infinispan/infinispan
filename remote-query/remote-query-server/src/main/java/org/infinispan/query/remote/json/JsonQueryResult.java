package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HITS;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @since 9.4
 */
public class JsonQueryResult extends JsonQueryResponse {

   private final List<Hit> hits;

   public JsonQueryResult(List<Hit> hits, long total) {
      super(total);
      this.hits = hits;
   }

   public List<Hit> getHits() {
      return hits;
   }

   @Override
   public Json toJson() {
      Json object = Json.object();
      object.set(TOTAL_RESULTS, getTotalResults());
      Json array = Json.array();
      hits.forEach(hit -> array.add(Json.factory().raw(hit.toJson().toString())));
      object.set(HITS, array);
      return object;
   }
}
