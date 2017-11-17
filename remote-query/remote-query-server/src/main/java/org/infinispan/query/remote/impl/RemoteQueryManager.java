package org.infinispan.query.remote.impl;

import java.util.List;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * Manages components used during indexed and index-less query.
 *
 * @since 9.2
 */
public interface RemoteQueryManager {

   /**
    * @return {@link Matcher} to be used during non-indexed query and filter operations.
    */
   Matcher getMatcher();

   /**
    * @return {@link QueryEngine}
    */
   BaseRemoteQueryEngine getQueryEngine();

   /**
    * @param queryRequest serialized {@link QueryRequest} provided by the remote client.
    * @return decoded {@link QueryRequest}.
    */
   QueryRequest decodeQueryRequest(byte[] queryRequest);

   /**
    * @param queryResponse {@link QueryResponse} carrying the result of the remote query.
    * @return encoded response to send back to the remote client.
    */
   byte[] encodeQueryResponse(QueryResponse queryResponse);

   /**
    * @param filterResult the {@link FilterResult} from filtering and continuous query operations.
    * @return Encoded FilterResult to send to the remote client.
    */
   Object encodeFilterResult(Object filterResult);

   /**
    * @return the {@link Encoder} associated with the cache's keys.
    */
   Encoder getKeyEncoder();

   /**
    * @return the {@link Encoder} associated with the cache's values.
    */
   Encoder getValueEncoder();

   /**
    * @return apply optional encoding to query hits
    */
   default List<Object> encodeQueryResults(List<Object> results) {
      return results;
   }

   default RemoteQueryResult executeQuery(String q, Integer offset, Integer maxResults) {
      BaseQuery baseQuery = getQueryEngine().makeQuery(q, null, offset, maxResults);
      List<Object> results = baseQuery.list();
      String[] projection = baseQuery.getProjection();
      int totalResults = baseQuery.getResultSize();
      if (projection == null) {
         return new RemoteQueryResult(null, totalResults, encodeQueryResults(results));
      } else {
         return new RemoteQueryResult(projection, totalResults, results);
      }
   }
}
