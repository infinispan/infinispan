package org.infinispan.api.common.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder class to build {@link QueryRequest}
 * <p>
 * Different parameters can be specified:
 * <u>
 * <li>Ickle Query: Mandatory query string</li>
 * <li>Param: Parameters for the query/li>
 * <li>Skip: Skip this number of entries</li>
 * <li>Limit: Limit the result to this number of entries</li>
 * </u>
 *
 * @since 14.0
 */
public final class QueryRequestBuilder {
   private final Map<String, Object> params;
   private String ickleQuery;
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

   public QueryRequestBuilder param(String name, Object value) {
      params.put(name, value);
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

   public QueryRequest find() {
      return new QueryRequest(ickleQuery, params, skip, limit);
   }
}
