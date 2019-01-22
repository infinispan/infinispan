package org.infinispan.api.reactive.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder class to build {@link QueryRequest} that can be used for Continuous Queries
 *
 * Different parameters can be specified:
 * <u>
 *    <li>Ickle Query: Mandatory query string</li>
 *    <li>Param: Parameters for the query/li>
 *    <li>All: Get created, updated and removed entries</li>
 *    <li>Created: Get created entries</li>
 *    <li>Updated: Get updated entries</li>
 *    <li>Deleted: Get deleted entries</li>
 * </u>
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public final class ContinuousQueryRequestBuilder {
   private String ickleQuery;
   private Map<String, Object> params;
   private boolean created;
   private boolean updated;
   private boolean deleted;

   private ContinuousQueryRequestBuilder() {
      params = new HashMap<>();
   }

   public static final ContinuousQueryRequestBuilder query(String ickleQuery) {
      ContinuousQueryRequestBuilder queryParameters = new ContinuousQueryRequestBuilder();
      queryParameters.ickleQuery = ickleQuery;
      return queryParameters;
   }

   public final ContinuousQueryRequestBuilder param(String name, Object value) {
      params.put(name, value);
      return this;
   }

   public final ContinuousQueryRequestBuilder all() {
      created = true;
      updated = true;
      deleted = true;
      return this;
   }

   public final ContinuousQueryRequestBuilder created() {
      created = true;
      return this;
   }

   public final ContinuousQueryRequestBuilder updated() {
      updated = true;
      return this;
   }

   public final ContinuousQueryRequestBuilder deleted() {
      deleted = true;
      return this;
   }

   public final QueryRequest build() {
      return new QueryRequest(created, updated, deleted, ickleQuery, params);
   }
}
