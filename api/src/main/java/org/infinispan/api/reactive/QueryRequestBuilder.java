package org.infinispan.api.reactive;

import java.util.HashMap;
import java.util.Map;

public final class QueryRequestBuilder {
   private String ickleQuery;
   private Map<String, Object> params;
   private boolean created;
   private boolean updated;
   private boolean deleted;
   private long skip;
   private int limit = -1;

   private QueryRequestBuilder() {
      params = new HashMap<>();
   }

   public static final QueryRequestBuilder query(String ickleQuery) {
      QueryRequestBuilder queryParameters = new QueryRequestBuilder();
      queryParameters.ickleQuery = ickleQuery;
      return queryParameters;
   }

   public final QueryRequestBuilder param(String name, Object value) {
      params.put(name, value);
      return this;
   }

   public final QueryRequestBuilder all() {
      created = true;
      updated = true;
      deleted = true;
      return this;
   }

   public final QueryRequestBuilder created() {
      created = true;
      return this;
   }

   public final QueryRequestBuilder updated() {
      updated = true;
      return this;
   }

   public final QueryRequestBuilder deleted() {
      deleted = true;
      return this;
   }

   public QueryRequestBuilder skip(long skip) {
      this.skip = skip;
      return this;
   }

   public QueryRequestBuilder limit(int limit) {
      this.limit = limit;
      return this;
   }

   public final QueryRequest build() {
      return new QueryRequest(created, updated, deleted, ickleQuery, params, skip, limit);
   }
}
