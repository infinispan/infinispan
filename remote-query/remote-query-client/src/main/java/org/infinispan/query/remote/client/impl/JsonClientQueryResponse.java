package org.infinispan.query.remote.client.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.protostream.SerializationContext;

/**
 * @since 9.4
 */
public class JsonClientQueryResponse implements BaseQueryResponse<String> {

   private static final String JSON_HITS = "hits";
   private static final String JSON_HIT = "hit";
   private static final String JSON_HIT_COUNT = "hit_count";
   private static final String JSON_HIT_COUNT_EXACT = "hit_count_exact";

   private final Json jsonObject;

   public JsonClientQueryResponse(Json jsonObject) {
      this.jsonObject = jsonObject;
   }

   @Override
   public List<String> extractResults(SerializationContext serializationContext) {
      return jsonObject.at(JSON_HITS).asJsonList().stream().map(j -> j.at(JSON_HIT).toString())
            .collect(Collectors.toList());
   }

   @Override
   public int hitCount() {
      return jsonObject.at(JSON_HIT_COUNT).asInteger();
   }

   @Override
   public boolean hitCountExact() {
      return jsonObject.at(JSON_HIT_COUNT_EXACT).asBoolean();
   }
}
