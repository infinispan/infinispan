package org.infinispan.api.reactive.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder class to build {@link QueryRequest}
 *
 * Different parameters can be specified:
 * <u>
 *    <li>Ickle Query: Mandatory query string</li>
 *    <li>Param: Parameters for the query/li>
 *    <li>Skip: Skip this number of entries</li>
 *    <li>Limit: Limit the result to this number of entries</li>
 * </u>
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public final class QueryRequestBuilder {
   private String ickleQuery;
   private Map<String, Object> params;
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

   public QueryRequestBuilder skip(long skip) {
      this.skip = skip;
      return this;
   }

   public QueryRequestBuilder limit(int limit) {
      this.limit = limit;
      return this;
   }

   public final QueryRequest build() {
      return new QueryRequest(ickleQuery, params, skip, limit);
   }
}
