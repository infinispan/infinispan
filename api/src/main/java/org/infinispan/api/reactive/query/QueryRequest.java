package org.infinispan.api.reactive.query;

import java.util.Map;

/**
 * QueryRequest creates a request for Query or Continuous Query.
 * It is built with the {@link QueryRequestBuilder}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public final class QueryRequest {
   private final boolean created;
   private final boolean updated;
   private final boolean removed;
   private final String ickleQuery;
   private final Map<String, Object> params;
   private final long skip;
   private final int limit;

   QueryRequest(boolean created, boolean updated, boolean removed,
                       String ickleQuery, Map<String, Object> params) {
      this.created = created;
      this.updated = updated;
      this.removed = removed;
      this.ickleQuery = ickleQuery;
      this.params = params;
      this.skip = 0;
      this.limit = -1;
   }

   public QueryRequest(String ickleQuery, Map<String, Object> params, long skip, int limit) {
      this.created = true;
      this.updated = true;
      this.removed = true;
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
