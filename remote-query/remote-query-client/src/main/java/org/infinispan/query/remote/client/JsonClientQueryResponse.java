package org.infinispan.query.remote.client;

import static java.util.stream.StreamSupport.stream;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.protostream.SerializationContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * @since 9.4
 */
public class JsonClientQueryResponse implements BaseQueryResponse {

   private static final String JSON_TOTAL_RESULTS = "total_results";
   private static final String JSON_HITS = "hits";
   private static final String JSON_HIT = "hit";

   private final JsonObject jsonObject;

   public JsonClientQueryResponse(JsonObject jsonObject) {
      this.jsonObject = jsonObject;
   }

   @Override
   public List<?> extractResults(SerializationContext serializationContext) {
      JsonArray hits = jsonObject.get(JSON_HITS).getAsJsonArray();
      return stream(hits.spliterator(), false)
            .map(hit -> hit.getAsJsonObject().get(JSON_HIT).toString())
            .collect(Collectors.toList());
   }

   @Override
   public long getTotalResults() {
      return jsonObject.get(JSON_TOTAL_RESULTS).getAsInt();
   }
}
