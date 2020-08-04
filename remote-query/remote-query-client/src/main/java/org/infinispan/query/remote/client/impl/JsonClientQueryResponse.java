package org.infinispan.query.remote.client.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.protostream.SerializationContext;

/**
 * @since 9.4
 */
public class JsonClientQueryResponse implements BaseQueryResponse<String> {

   private static final String JSON_TOTAL_RESULTS = "total_results";
   private static final String JSON_HITS = "hits";
   private static final String JSON_HIT = "hit";

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
   public long getTotalResults() {
      return jsonObject.at(JSON_TOTAL_RESULTS).asInteger();
   }
}
