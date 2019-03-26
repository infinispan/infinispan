package org.infinispan.api.search.reactive;

import java.util.HashMap;
import java.util.Map;

public class QueryParameters {
   private Map<String, Object> params;

   private QueryParameters() {
      params = new HashMap<>();
   }

   public static final QueryParameters init(String name, Object value) {
      QueryParameters queryParameters = new QueryParameters();
      queryParameters.params.put(name, value);
      return queryParameters;
   }

   public final QueryParameters append(String name, Object value) {
      params.put(name, value);
      return this;
   }

   public final Map<String, Object> asMap() {
      return params;
   }
}
