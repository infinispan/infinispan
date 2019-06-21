package org.infinispan.api.reactive;

import java.util.Map;

public class QueryRequest {
   private final boolean created;
   private final boolean updated;
   private final boolean removed;
   private final String ickleQuery;
   private final Map<String, Object> params;
   private final long skip;
   private final int limit;

   public QueryRequest(boolean created, boolean updated, boolean removed,
                       String ickleQuery, Map<String, Object> params,
                       long skip, int limit) {
      this.created = created;
      this.updated = updated;
      this.removed = removed;
      this.ickleQuery = ickleQuery;
      this.params = params;
      this.skip = skip;
      this.limit = limit;
   }

   public boolean isCreated() {
      return created;
   }

   public boolean isUpdated() {
      return updated;
   }

   public boolean isDeleted() {
      return removed;
   }

   public String getIckleQuery() {
      return ickleQuery;
   }

   public Map<String, Object> getParams() {
      return params;
   }

   public long skip() {
      return skip;
   }

   public int limit() {
      return limit;
   }
}
